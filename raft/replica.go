package raft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/chans"
	"github.com/pkopriv2/golang-sdk/lang/concurrent"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/pool"
	uuid "github.com/satori/go.uuid"
)

// The replica is the central state container for a host within a cluster.  The
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

	// client pools
	clientPools concurrent.Map

	// the current term.
	term Term

	// The core options for the replica
	Options Options

	// the election timeout.  usually a random offset from a shared value
	ElectionTimeout time.Duration

	// read barrier request
	BarrierRequests chan *chans.Request

	// request vote events.
	VoteRequests chan *chans.Request

	// replication requests
	ReplicationRequests chan *chans.Request

	// snapshot install (presumably from leader)
	SnapshotRequests chan *chans.Request

	// append requests (from local state machine or remote client)
	AppendRequests chan *chans.Request

	// roster update requests
	RosterUpdateRequests chan *chans.Request
}

func newReplica(ctx context.Context, store LogStorage, termStore PeerStorage, addr string, opts Options) (*replica, error) {

	id, err := getOrCreateReplicaId(termStore, addr)
	if err != nil {
		return nil, errors.Wrapf(err, "Error retrieving peer id [%v]", addr)
	}

	self := Peer{id, addr}
	ctx = ctx.Sub("%v", self)

	roster := newRoster(NewPeers([]Peer{self}))
	ctx.Control().Defer(func(cause error) {
		roster.Close()
	})

	log, err := getOrCreateLog(ctx, store, self)
	if err != nil {
		return nil, errors.Wrapf(err, "Error retrieving durable log [%v]", id)
	}

	offset, err := rand.Int(rand.Reader, big.NewInt(int64(opts.ElectionTimeout/2)))
	if err != nil {
		return nil, errors.Wrapf(err, "Error generating election timeout")
	}

	r := &replica{
		Ctx:                  ctx,
		logger:               ctx.Logger(),
		ctrl:                 ctx.Control(),
		Self:                 self,
		Terms:                termStore,
		Log:                  log,
		Roster:               roster,
		clientPools:          concurrent.NewMap(),
		BarrierRequests:      make(chan *chans.Request),
		ReplicationRequests:  make(chan *chans.Request),
		VoteRequests:         make(chan *chans.Request),
		AppendRequests:       make(chan *chans.Request),
		SnapshotRequests:     make(chan *chans.Request),
		RosterUpdateRequests: make(chan *chans.Request),
		ElectionTimeout:      opts.ElectionTimeout/2 + time.Duration(offset.Int64()),
		Options:              opts,
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

func (r *replica) NewRootContext() context.Context {
	return context.NewContext(r.Ctx.Logger().Out(), r.Ctx.Logger().Level())
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

func (r *replica) FindPeer(id uuid.UUID) (ret Peer, ok bool) {
	ret, ok = r.Cluster()[id]
	return
}

func (r *replica) Others() Peers {
	return r.Cluster().Delete(r.Self)
}

func (r *replica) Majority() int {
	return majority(len(r.Cluster()))
}

type broadCastResponse struct {
	Peer Peer
	Err  error
	Val  interface{}
}

func (r *replica) getClientPool(p Peer) (ret pool.ObjectPool) {
	raw, ok := r.clientPools.Try(p.Id)
	if ok {
		ret = raw.(pool.ObjectPool)
		return
	}

	r.clientPools.Update(func(m concurrent.Map) {
		raw, ok := m.Try(p.Id)
		if ok {
			ret = raw.(pool.ObjectPool)
			return
		}
		ret = p.ClientPool(r.ctrl, r.Options)
		m.Put(p.Id, ret)
	})
	return
}

func (r *replica) Broadcast(fn func(c *Client) (interface{}, error)) <-chan broadCastResponse {
	peers := r.Others()

	ret := make(chan broadCastResponse, len(peers))
	for _, p := range peers {
		go func(p Peer) {
			pool := r.getClientPool(p)
			var err error

			cl, err := pool.TakeOrCancel(r.ctrl.Closed())
			if err != nil {
				ret <- broadCastResponse{Peer: p, Err: err}
				return
			}
			defer func() {
				if err != nil {
					pool.Fail(cl)
				} else {
					pool.Return(cl)
				}
			}()

			val, err := fn(cl.(*Client))
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
	timer := context.NewTimer(r.ctrl, timeout)
	defer timer.Close()
	return chans.SendRequest(r.ctrl, ch, timer.Closed(), val)
}

func (r *replica) Listen(start int64, buf int64) (Listener, error) {
	return r.Log.ListenCommits(start, buf)
}

func (r *replica) Compact(cancel <-chan struct{}, until int64, data <-chan Event) error {
	return r.Log.Compact(cancel, until, data, Config{r.Cluster()})
}

func (r *replica) Append(req AppendRequest) (ret Entry, err error) {
	val, err := r.sendRequest(r.AppendRequests, r.Options.ReadTimeout, req)
	if err != nil {
		return
	}
	return val.(Entry), nil
}

func (r *replica) UpdateRoster(update RosterUpdateRequest) error {
	_, err := r.sendRequest(r.RosterUpdateRequests, r.Options.ReadTimeout, update)
	return err
}

func (r *replica) ReadBarrier() (ret ReadBarrierResponse, err error) {
	val, err := r.sendRequest(r.BarrierRequests, r.Options.ReadTimeout, nil)
	if err != nil {
		return
	}
	return val.(ReadBarrierResponse), nil
}

func (r *replica) InstallSnapshot(snapshot InstallSnapshotRequest) (ret InstallSnapshotResponse, err error) {
	val, err := r.sendRequest(r.SnapshotRequests, r.Options.ReadTimeout, snapshot)
	if err != nil {
		return InstallSnapshotResponse{}, err
	}
	return val.(InstallSnapshotResponse), nil
}

func (r *replica) Replicate(req ReplicateRequest) (ReplicateResponse, error) {
	val, err := r.sendRequest(r.ReplicationRequests, r.Options.ReadTimeout, req)
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
		raw, err = store.NewLog(self.Id, Config{NewPeers([]Peer{self})})
		if err != nil {
			err = errors.Wrapf(err, "Error opening stored log [%v]", self.Id)
			return
		}
	}

	return openLog(ctx, raw)
}
