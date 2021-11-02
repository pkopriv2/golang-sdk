package raft

import (
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/chans"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkopriv2/golang-sdk/lang/pool"
	uuid "github.com/satori/go.uuid"
)

// The replica is the state container for a member of a cluster.  The
// replica is managed by a single member of the replicated log state machine
// network.  However, the replica is also a machine itself.  Consumers can
// interact with it and it can respond to its own state changes.
//
// The replica is the primary gatekeeper to the external state machine
// and it manages the flow of data to/from it.
type replica struct {

	// configuration used to build this instance.
	Ctx context.Context

	// the control (de-normalized from Ctx.Logger())
	logger context.Logger

	// the control (de-normalized from Ctx.Control())
	ctrl context.Control

	// the peer representing the local instance
	Self Peer

	// the current cluster configuration
	Roster *roster

	// the event log.
	Log *entryLog

	// the durable term store.
	Terms *termStore

	// data lock (currently using very coarse lock)
	lock sync.RWMutex

	// the current term.
	term term

	// The core options for the replica
	Options Options

	// the election timeout.  usually a random offset from a shared value
	ElectionTimeout time.Duration

	// read barrier request
	Barrier chan *chans.Request

	// request vote events.
	VoteRequests chan *chans.Request

	// append requests (presumably from leader)
	Replications chan *chans.Request

	// snapshot install (presumably from leader)
	Snapshots chan *chans.Request

	// append requests (from clients)
	RemoteAppends chan *chans.Request

	// append requests (from local state machine)
	LocalAppends chan *chans.Request

	// append requests (from local state machine)
	RosterUpdates chan *chans.Request
}

func newReplica(ctx context.Context, store LogStore, termStore *termStore, addr string, opts Options) (*replica, error) {

	id, err := getOrCreateReplicaId(termStore, addr)
	if err != nil {
		return nil, errors.Wrapf(err, "Error retrieving peer id [%v]", addr)
	}

	self := Peer{id, addr}
	ctx = ctx.Sub("%v", self)
	ctx.Logger().Info("Starting replica.")

	log, err := getOrCreateLog(ctx, store, self)
	if err != nil {
		return nil, errors.Wrapf(err, "Error retrieving durable log [%v]", id)
	}
	ctx.Control().Defer(func(cause error) {
		log.Close()
	})

	roster := newRoster([]Peer{self})
	ctx.Control().Defer(func(cause error) {
		roster.Close()
	})

	ctx.Control().Defer(func(cause error) {
		ctx.Logger().Info("Replica closed: %v", cause)
	})

	rndmElectionTimeout := time.Duration(int64(rand.Intn(1000000000)))
	r := &replica{
		Ctx:             ctx,
		logger:          ctx.Logger(),
		ctrl:            ctx.Control(),
		Self:            self,
		Terms:           termStore,
		Log:             log,
		Roster:          roster,
		Barrier:         make(chan *chans.Request),
		Replications:    make(chan *chans.Request),
		VoteRequests:    make(chan *chans.Request),
		RemoteAppends:   make(chan *chans.Request),
		LocalAppends:    make(chan *chans.Request),
		Snapshots:       make(chan *chans.Request),
		RosterUpdates:   make(chan *chans.Request),
		ElectionTimeout: opts.ElectionTimeout + rndmElectionTimeout,
		Options:         opts,
	}
	return r, r.start()
}

func (r *replica) Close() error {
	return r.ctrl.Close()
}

func (r *replica) start() error {
	// retrieve the term from the durable store
	term, _, err := r.Terms.Get(r.Self.Id)
	if err != nil {
		return err
	}

	// set the term from durable storage.
	if err := r.SetTerm(term.Num, term.LeaderId, term.VotedFor); err != nil {
		return err
	}

	listenRosterChanges(r)
	return nil
}

func (r *replica) String() string {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return fmt.Sprintf("%v, %v:", r.Self, r.term)
}

func (r *replica) SetTerm(num int64, leader *uuid.UUID, vote *uuid.UUID) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.term = term{num, leader, vote}
	r.logger.Info("Durably storing updated term [%v]", r.term)
	return r.Terms.Save(r.Self.Id, r.term)
}

func (r *replica) CurrentTerm() term {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.term
}

func (r *replica) Cluster() Peers {
	all, _ := r.Roster.Get()
	return all
}

func (r *replica) Leader() *Peer {
	if term := r.CurrentTerm(); term.LeaderId != nil {
		peer, found := r.FindPeer(*term.LeaderId)
		if !found {
			return nil
		} else {
			return &peer
		}
	}
	return nil
}

func (r *replica) FindPeer(id uuid.UUID) (Peer, bool) {
	for _, p := range r.Cluster() {
		if p.Id == id {
			return p, true
		}
	}
	return Peer{}, false
}

func (r *replica) Others() []Peer {
	cluster := r.Cluster()

	others := make([]Peer, 0, len(cluster))
	for _, p := range cluster {
		if p.Id != r.Self.Id {
			others = append(others, p)
		}
	}
	return others
}

func (r *replica) Majority() int {
	return majority(len(r.Cluster()))
}

type broadCastResponse struct {
	Err error
	Val interface{}
}

func (r *replica) Broadcast(fn func(c *rpcClient) (interface{}, error)) <-chan broadCastResponse {
	peers := r.Others()
	ret := make(chan broadCastResponse, len(peers))
	for _, p := range peers {
		go func(p Peer) {
			cl, err := p.Dial(r.Options)
			if err != nil {
				ret <- broadCastResponse{Err: err}
				return
			}

			defer cl.Close()
			val, err := fn(cl)
			if err != nil {
				ret <- broadCastResponse{Err: err}
				return
			}

			ret <- broadCastResponse{Val: val}
		}(p)
	}
	return ret
}

func (r *replica) sendRequest(ch chan<- *chans.Request, timeout time.Duration, val interface{}) (interface{}, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	req := chans.NewRequest(val)
	defer req.Cancel()

	select {
	case <-r.ctrl.Closed():
		return nil, errors.WithStack(errs.ClosedError)
	case <-timer.C:
		return nil, errors.Wrapf(errs.TimeoutError, "Request timed out waiting for machine to accept [%v]", timeout)
	case ch <- req:
		select {
		case <-r.ctrl.Closed():
			return nil, errors.WithStack(errs.ClosedError)
		case r := <-req.Acked():
			return r, nil
		case e := <-req.Failed():
			return nil, errors.Wrap(e, "Request failed")
		case <-timer.C:
			return nil, errors.Wrapf(errs.TimeoutError, "Request timed out waiting for machine to response [%v]", timeout)
		}
	}
}

func (r *replica) AddPeer(peer Peer) error {
	return r.UpdateRoster(rosterUpdateRequest{peer, true})
}

func (r *replica) DelPeer(peer Peer) error {
	return r.UpdateRoster(rosterUpdateRequest{peer, false})
}

func (r *replica) Append(event Event, kind Kind) (Entry, error) {
	return r.LocalAppend(appendEventRequest{event, kind})
}

func (r *replica) Listen(start int64, buf int64) (Listener, error) {
	return r.Log.ListenCommits(start, buf)
}

func (r *replica) Compact(until int64, data <-chan Event) error {
	return r.Log.Compact(until, data, Config{r.Cluster()})
}

func (r *replica) UpdateRoster(update rosterUpdateRequest) error {
	_, err := r.sendRequest(r.RosterUpdates, 30*time.Second, update)
	return err
}

func (r *replica) ReadBarrier() (int64, error) {
	val, err := r.sendRequest(r.Barrier, r.Options.ReadTimeout, nil)
	if err != nil {
		return 0, err
	}
	return val.(int64), nil
}

func (r *replica) InstallSnapshot(snapshot installSnapshotRequest) (installSnapshotResponse, error) {
	val, err := r.sendRequest(r.Snapshots, r.Options.ReadTimeout, snapshot)
	if err != nil {
		return installSnapshotResponse{}, err
	}
	return val.(installSnapshotResponse), nil
}

func (r *replica) Replicate(req replicateRequest) (replicateResponse, error) {
	val, err := r.sendRequest(r.Replications, r.Options.ReadTimeout, req)
	if err != nil {
		return replicateResponse{}, err
	}
	return val.(replicateResponse), nil
}

func (r *replica) RequestVote(vote voteRequest) (voteResponse, error) {
	val, err := r.sendRequest(r.VoteRequests, r.Options.ReadTimeout, vote)
	if err != nil {
		return voteResponse{}, err
	}
	return val.(voteResponse), nil
}

func (r *replica) RemoteAppend(event appendEventRequest) (Entry, error) {
	val, err := r.sendRequest(r.RemoteAppends, r.Options.ReadTimeout, event)
	if err != nil {
		return Entry{}, err
	}
	return val.(Entry), nil
}

func (r *replica) LocalAppend(event appendEventRequest) (Entry, error) {
	val, err := r.sendRequest(r.LocalAppends, r.Options.ReadTimeout, event)
	if err != nil {
		return Entry{}, err
	}
	return val.(Entry), nil
}

func newLeaderPool(self *replica, size int) pool.ObjectPool {
	return pool.NewObjectPool(self.Ctx.Control(), size, func() (io.Closer, error) {
		var cl *rpcClient
		for cl == nil {
			leader := self.Leader()
			if leader == nil {
				time.Sleep(self.ElectionTimeout / 5)
				continue
			}

			cl, _ = leader.Dial(self.Options)
		}
		return cl, nil
	})
}

func getOrCreateReplicaId(store *termStore, addr string) (uuid.UUID, error) {
	id, ok, err := store.GetId(addr)
	if err != nil {
		return uuid.UUID{}, errors.Wrapf(err, "Error retrieving id for address [%v]", addr)
	}

	if !ok {
		id = uuid.NewV1()
		if err := store.SetId(addr, id); err != nil {
			return uuid.UUID{}, errors.Wrapf(err, "Error associating addr [%v] with id [%v]", addr, id)
		}
	}

	return id, nil
}

func getOrCreateLog(ctx context.Context, store LogStore, self Peer) (*entryLog, error) {
	raw, err := store.GetLog(self.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Error opening stored log [%v]", self.Id)
	}

	if raw == nil {
		raw, err = store.NewLog(self.Id, Config{[]Peer{self}})
		if err != nil {
			return nil, errors.Wrapf(err, "Error opening stored log [%v]", self.Id)
		}
	}

	return openEntryLog(ctx, raw)
}
