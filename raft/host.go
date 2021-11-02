package raft

import (
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/pool"
	"github.com/pkopriv2/golang-sdk/rpc"
	uuid "github.com/satori/go.uuid"
)

//// FIXME: Need to discover self address from remote address.

// a host simply binds a network service with the core log machine.
type host struct {
	ctx        context.Context
	ctrl       context.Control
	logger     context.Logger
	server     rpc.Server
	replica    *replica
	sync       *syncer
	leaderPool pool.ObjectPool // T: *rpcClient
}

func newHost(ctx context.Context, addr string, opts Options) (h *host, err error) {
	ctx = ctx.Sub("RaftHost")
	defer func() {
		if err != nil {
			ctx.Control().Fail(err)
		}
	}()

	store, err := opts.LogStorage()
	if err != nil {
		return nil, err
	}

	terms, err := opts.TermStorage()
	if err != nil {
		return nil, err
	}

	listener, err := opts.Network.Listen(addr)
	if err != nil {
		return nil, err
	}
	ctx.Control().Defer(func(cause error) {
		listener.Close()
	})

	replica, err := newReplica(ctx, store, terms, listener.Address().String(), opts)
	if err != nil {
		return nil, err
	}
	ctx.Control().Defer(func(cause error) {
		replica.Close()
	})

	server, err := newServer(ctx, replica, listener, replica.Options.Workers)
	if err != nil {
		return nil, err
	}
	ctx.Control().Defer(func(cause error) {
		server.Close()
	})

	pool := newLeaderPool(replica, 10)
	ctx.Control().Defer(func(cause error) {
		pool.Close()
	})

	sync := newSyncer(pool)
	ctx.Control().Defer(func(cause error) {
		sync.Close()
	})

	return &host{
		ctx:        ctx,
		ctrl:       ctx.Control(),
		logger:     ctx.Logger(),
		replica:    replica,
		server:     server,
		leaderPool: pool,
		sync:       sync,
	}, nil
}

func (h *host) Fail(e error) error {
	h.ctrl.Fail(e)
	return h.ctrl.Failure()
}

func (h *host) Close() error {
	return h.Fail(h.Leave())
}

func (h *host) Id() uuid.UUID {
	return h.replica.Self.Id
}

func (h *host) Context() context.Context {
	return h.replica.Ctx
}

func (h *host) Addr() string {
	return h.replica.Self.Addr
}

func (h *host) Self() Peer {
	return h.replica.Self
}

func (h *host) Peers() []Peer {
	return h.replica.Others()
}

func (h *host) Cluster() []Peer {
	return h.replica.Cluster()
}

func (h *host) Roster() []Peer {
	return h.replica.Cluster()
}

func (h *host) Sync() (Sync, error) {
	return h.sync, nil
}

func (h *host) Log() (Log, error) {
	return newLogClient(h.replica, h.leaderPool), nil
}

func (h *host) Addrs() []string {
	addrs := make([]string, 0, 8)
	for _, p := range h.replica.Cluster() {
		addrs = append(addrs, p.Addr)
	}
	return addrs
}

func (h *host) Start() error {
	becomeFollower(h.replica)
	return nil
}

func (h *host) Join(addr string) error {
	var err error

	becomeFollower(h.replica)
	defer func() {
		if err != nil {
			h.replica.ctrl.Fail(err)
			h.ctx.Logger().Error("Error joining: %v", err)
		}
	}()

	for attmpt := 0; attmpt < 3; attmpt++ {
		err = h.tryJoin(addr)
		if err != nil {
			h.ctx.Logger().Error("Attempt(%v): Error joining cluster: %v: %v", addr, attmpt, err)
			continue
		}
		break
	}

	return err
}

func (h *host) Leave() error {
	var err error
	for attmpt := 0; attmpt < 3; attmpt++ {
		err = h.tryLeave()
		if err != nil {
			h.ctx.Logger().Error("Attempt(%v): Error leaving cluster: %v", attmpt, err)
			continue
		}
		break
	}

	h.ctx.Logger().Info("Shutting down: %v", err)
	h.replica.ctrl.Fail(err)
	return err
}

func (h *host) tryJoin(addr string) error {
	cl, err := dialRpcClient(
		h.replica.Options.Network,
		h.replica.Options.ReadTimeout,
		addr,
		h.replica.Options.Encoder)
	if err != nil {
		return errors.Wrapf(err, "Error connecting to peer [%v]", addr)
	}
	defer cl.Close()

	status, err := cl.Status()
	if err != nil {
		return errors.Wrapf(err, "Error getting cluster status from [%v]", addr)
	}

	// this may not be necessary...
	if err := h.replica.SetTerm(status.Term.Num, status.LeaderId, status.LeaderId); err != nil {
		return errors.Wrap(err, "Error updating term information")
	}
	return cl.UpdateRoster(h.replica.Self, true)
}

func (h *host) tryLeave() error {
	peer := h.replica.Leader()
	if peer == nil {
		return ErrNoLeader
	}

	cl, err := peer.Dial(h.replica.Options)
	if err != nil {
		return err
	}
	defer cl.Close()
	return cl.UpdateRoster(h.replica.Self, false)
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

// This is the public facing client.  Only emits committed items.
type logClient struct {
	id         uuid.UUID
	ctx        context.Context
	ctrl       context.Control
	logger     context.Logger
	leaderPool pool.ObjectPool // T: *rpcClient
	self       *replica
}

func newLogClient(self *replica, leaderPool pool.ObjectPool) *logClient {
	ctx := self.Ctx.Sub("LogClient")
	return &logClient{
		id:         self.Self.Id,
		ctx:        ctx,
		logger:     ctx.Logger(),
		ctrl:       ctx.Control(),
		self:       self,
		leaderPool: leaderPool,
	}
}

func (s *logClient) Id() uuid.UUID {
	return s.id
}

func (c *logClient) Close() error {
	return c.ctrl.Close()
}

func (s *logClient) Head() int64 {
	return s.self.Log.Head()
}

func (s *logClient) Committed() int64 {
	return s.self.Log.Committed()
}

func (s *logClient) Compact(until int64, data <-chan Event) error {
	return s.self.Compact(until, data)
}

func (s *logClient) Listen(start int64, buf int64) (Listener, error) {
	raw, err := s.self.Log.ListenCommits(start, buf)
	if err != nil {
		return nil, err
	}
	return newLogClientListener(raw), nil
}

func (c *logClient) Append(cancel <-chan struct{}, e []byte) (Entry, error) {
	return c.append(cancel, e, Std)
}

func (s *logClient) Snapshot() (int64, EventStream, error) {
	snapshot, err := s.self.Log.Snapshot()
	if err != nil {
		return 0, nil, err
	}
	return snapshot.LastIndex(), newSnapshotStream(s.ctrl, snapshot, 1024), nil
}

func (c *logClient) append(cancel <-chan struct{}, payload []byte, kind Kind) (entry Entry, err error) {
	for {
		raw := c.leaderPool.TakeOrCancel(cancel)
		if raw == nil {
			err = ErrCanceled
			return
		}

		// FIXME: Implement exponential backoff
		resp, e := raw.(*rpcClient).Append(appendEventRequest{payload, kind})
		if e != nil {
			c.leaderPool.Fail(raw)
			continue
		}

		entry = Entry{
			Kind:    Std,
			Term:    resp.Term,
			Index:   resp.Index,
			Payload: payload,
		}

		c.leaderPool.Return(raw)
		return
	}
}

// This client filters out the low-level entries and replaces
// them with NoOp instructions to the consuming state machine.
type logClientListener struct {
	raw Listener
	dat chan Entry
}

func newLogClientListener(raw Listener) *logClientListener {
	l := &logClientListener{raw, make(chan Entry)}
	l.start()
	return l
}

func (p *logClientListener) start() {
	go func() {
		for {
			var e Entry
			select {
			case <-p.raw.Ctrl().Closed():
				return
			case e = <-p.raw.Data():
			}

			if e.Kind != Std {
				e = Entry{
					Kind:    NoOp,
					Term:    e.Term,
					Index:   e.Index,
					Payload: []byte{},
				}
			}

			select {
			case <-p.raw.Ctrl().Closed():
				return
			case p.dat <- e:
			}
		}
	}()
}

func (l *logClientListener) Close() error {
	return l.raw.Close()
}

func (l *logClientListener) Ctrl() context.Control {
	return l.raw.Ctrl()
}

func (l *logClientListener) Data() <-chan Entry {
	return l.dat
}
