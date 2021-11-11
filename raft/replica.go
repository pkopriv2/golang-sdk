package raft

import (
	"fmt"
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

	// the core event log
	Log *log

	// the durable term store.
	Terms PeerStorage

	// data lock (currently using very coarse lock)
	lock sync.RWMutex

	// a leader pool
	leaderPool pool.ObjectPool

	// the current term.
	term Term

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

	// append requests (from local state machine)
	Appends chan *chans.Request

	// append requests (from local state machine)
	RosterUpdates chan *chans.Request
}

func newReplica(ctx context.Context, store LogStorage, termStore PeerStorage, addr string, opts Options) (*replica, error) {

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
		Appends:         make(chan *chans.Request),
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
	term, _, err := r.Terms.GetActiveTerm(r.Self.Id)
	if err != nil {
		return errors.Wrapf(err, "Unable to get latest term [%v]", r.Self.Id)
	}

	// set the term from durable storage.
	if err := r.SetTerm(term.Epoch, term.LeaderId, term.VotedFor); err != nil {
		return errors.Wrapf(err, "Unable to set latest term [%v]", r.Self.Id)
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
	r.term = Term{num, leader, vote}
	r.logger.Info("Updated to term [%v]", r.term)
	return r.Terms.SetActiveTerm(r.Self.Id, r.term)
}

func (r *replica) CurrentTerm() Term {
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
	Peer Peer
	Err  error
	Val  interface{}
}

func (r *replica) Broadcast(fn func(c Client) (interface{}, error)) <-chan broadCastResponse {
	peers := r.Others()
	ret := make(chan broadCastResponse, len(peers))
	for _, p := range peers {
		go func(p Peer) {
			cl, err := p.Dial(r.Options)
			if err != nil {
				ret <- broadCastResponse{Peer: p, Err: err}
				return
			}

			defer cl.Close()
			val, err := fn(cl)
			if err != nil {
				ret <- broadCastResponse{Peer: p, Err: err}
				return
			}

			ret <- broadCastResponse{Peer: p, Val: val}
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

func (r *replica) Listen(start int64, buf int64) (Listener, error) {
	return r.Log.ListenCommits(start, buf)
}

func (r *replica) Compact(cancel <-chan struct{}, until int64, data <-chan Event) error {
	return r.Log.Compact(cancel, until, data, Config{r.Cluster()})
}

func (r *replica) Append(req AppendEventRequest) (ret Entry, err error) {
	val, err := r.sendRequest(r.Appends, r.Options.ReadTimeout, req)
	if err != nil {
		return
	}
	return val.(Entry), nil
}

func (r *replica) UpdateRoster(update RosterUpdateRequest) error {
	_, err := r.sendRequest(r.RosterUpdates, r.Options.ReadTimeout, update)
	return err
}

func (r *replica) ReadBarrier() (ret ReadBarrierResponse, err error) {
	val, err := r.sendRequest(r.Barrier, r.Options.ReadTimeout, nil)
	if err != nil {
		return
	}
	return val.(ReadBarrierResponse), nil
}

func (r *replica) InstallSnapshot(snapshot InstallSnapshotRequest) (ret InstallSnapshotResponse, err error) {
	val, err := r.sendRequest(r.Snapshots, r.Options.ReadTimeout, snapshot)
	if err != nil {
		return InstallSnapshotResponse{}, err
	}
	return val.(InstallSnapshotResponse), nil
}

func (r *replica) Replicate(req ReplicateRequest) (ReplicateResponse, error) {
	val, err := r.sendRequest(r.Replications, r.Options.ReadTimeout, req)
	if err != nil {
		return ReplicateResponse{}, err
	}
	return val.(ReplicateResponse), nil
}

func (r *replica) RequestVote(vote VoteRequest) (VoteResponse, error) {
	val, err := r.sendRequest(r.VoteRequests, r.Options.ReadTimeout, vote)
	if err != nil {
		return VoteResponse{}, err
	}
	return val.(VoteResponse), nil
}

func getOrCreateReplicaId(store PeerStorage, addr string) (id uuid.UUID, err error) {
	id, ok, err := store.GetPeerId(addr)
	if err != nil {
		err = errors.Wrapf(err, "Error retrieving id for address [%v]", addr)
		return
	}

	if !ok {
		id = uuid.NewV1()
		if err = store.SetPeerId(addr, id); err != nil {
			err = errors.Wrapf(err, "Error associating addr [%v] with id [%v]", addr, id)
			return
		}
	}
	return
}

func getOrCreateLog(ctx context.Context, store LogStorage, self Peer) (ret *log, err error) {
	raw, err := store.GetLog(self.Id)
	if err != nil {
		err = errors.Wrapf(err, "Error opening stored log [%v]", self.Id)
		return
	}

	if raw == nil {
		raw, err = store.NewLog(self.Id, Config{[]Peer{self}})
		if err != nil {
			err = errors.Wrapf(err, "Error opening stored log [%v]", self.Id)
			return
		}
	}

	return openLog(ctx, raw)
}
