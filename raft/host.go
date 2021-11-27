package raft

import (
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/badgerdb"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkopriv2/golang-sdk/lang/net"
	"github.com/pkopriv2/golang-sdk/lang/pool"
	uuid "github.com/satori/go.uuid"
)

// A host implements the Host abstraction as defined in api.go.
type host struct {
	ctx        context.Context
	ctrl       context.Control
	logger     context.Logger
	server     *server
	replica    *replica
	sync       *syncer
	leaderPool pool.ObjectPool // T: Client
}

func newHost(ctx context.Context, addr string, opts Options) (h *host, err error) {
	ctx = ctx.Sub("Raft")
	defer func() {
		if err != nil {
			ctx.Control().Fail(err)
		}
	}()

	if opts.LogStorage == nil {
		db, err := badgerdb.OpenTemp()
		if err != nil {
			return nil, err
		}
		ctx.Control().Defer(func(error) {
			badgerdb.CloseAndDelete(db)
		})

		opts = opts.Update(WithLogStorage(NewBadgerLogStorage(db)))
	}

	if opts.PeerStorage == nil {
		db, err := badgerdb.OpenTemp() // TODO: reuse db for both log and term store
		if err != nil {
			return nil, err
		}
		ctx.Control().Defer(func(error) {
			badgerdb.CloseAndDelete(db)
		})

		opts = opts.Update(WithPeerStorage(NewBadgerPeerStorage(db)))
	}

	if opts.Transport == nil {
		opts = opts.Update(WithTransport(NewRpcTransport(net.NewTCP4Network(), enc.Gob)))
	}

	socket, err := opts.Transport.Listen(addr)
	if err != nil {
		return
	}
	ctx.Control().Defer(func(error) {
		socket.Close()
	})

	replica, err := newReplica(ctx, opts.LogStorage, opts.PeerStorage, socket.Addr(), opts)
	if err != nil {
		return
	}

	pool := newLeaderPool(replica, opts.MaxConnsPerPeer)
	ctx.Control().Defer(func(cause error) {
		pool.Close()
	})

	sync := newSyncer(pool)
	ctx.Control().Defer(func(cause error) {
		sync.Close()
	})

	// We need to forcefully close the host if the replica closed
	go func() {
		select {
		case <-ctx.Control().Closed():
		case <-replica.ctrl.Closed():
			ctx.Control().Fail(replica.ctrl.Failure())
		}
	}()

	h = &host{
		ctx:        ctx,
		ctrl:       ctx.Control(),
		logger:     ctx.Logger(),
		replica:    replica,
		server:     newServer(ctx, replica, socket, opts),
		leaderPool: pool,
		sync:       sync,
	}
	return
}

func (h *host) Kill() error {
	h.ctrl.Fail(nil)
	return h.ctrl.Failure()
}

func (h *host) Close() error {
	h.ctrl.Close()
	return h.ctrl.Failure()
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

func (h *host) Roster() Peers {
	return h.replica.Cluster()
}

func (h *host) Term() Term {
	return h.replica.CurrentTerm()
}

func (h *host) Sync() (Sync, error) {
	return h.sync, nil
}

func (h *host) Log() (Log, error) {
	return newLogClient(h.ctx, h.replica, h.leaderPool), nil
}

func (h *host) Leave() error {
	err := h.tryLeave(nil)
	h.replica.ctrl.Fail(err)
	return err
}

func (h *host) start() error {
	becomeFollower(h.replica)
	return nil
}

func (h *host) join(addrs []string) (err error) {
	logger := h.replica.logger

	wait := func() error {
		timer := time.NewTimer(h.replica.Options.ElectionTimeout)
		defer timer.Stop()
		select {
		case <-h.ctrl.Closed():
			return ErrClosed
		case <-timer.C:
			return nil
		}
	}

	failures, started := []error{}, false
	for i := 0; i < 10; i++ {
		for j := 0; j < len(addrs); j++ {
			logger.Info("Attempt [%v] to join [%v]: (errs=%v)", (i+1)*(j+1)+j, addrs[j], failures)

			cl, err := Dial(h.replica.Options.Transport, addrs[j], h.replica.Options.Timeouts())
			if err != nil {
				failures = append(failures, err)
				continue
			}

			status, err := cl.Status()
			if err != nil {
				cl.Close() // already in failure. No great mechanism for suppressed exception
				failures = append(failures, err)
				continue
			}

			// This is a race condition!!  We can set the roster and then
			// receive a roster update that removes us from the cluster.
			// Not sure yet what to do about this.
			h.replica.Roster.Set(status.Config.Peers)
			if status.Config.Peers.Contains(h.replica.Self) {
				cl.Close()
				return nil
			}

			h.replica.Roster.Set(status.Config.Peers.Add(h.replica.Self))
			if status.Term.LeaderId == nil {
				cl.Close()

				if e := wait(); e != nil {
					return e
				}

				failures = append(failures, errors.Wrapf(ErrNoLeader, "No leader according to [%v]", status.Self))
				continue
			}

			leader, ok := status.Config.Peers[*status.Term.LeaderId]
			if !ok {
				cl.Close()

				if e := wait(); e != nil {
					return e
				}

				failures = append(failures, errors.Wrapf(ErrNoLeader, "Could not locate leader [%v]", status.Term.LeaderId))
				continue
			}

			if status.Self.Id != leader.Id {
				cl.Close()

				cl, err = leader.Dial(h.replica.Options)
				if err != nil {
					failures = append(failures, errors.Wrapf(err, "Error dialing leader [%v]", leader))
					continue
				}
			}

			if !started {
				becomeFollower(h.replica)
			}

			if err = cl.UpdateRoster(RosterUpdateRequest{h.replica.Self, true}); err != nil {
				cl.Close()

				if errs.Is(err, ErrNotLeader) {
					if e := wait(); e != nil {
						return e
					}
				}

				failures = append(failures, errors.Wrapf(err, "Error joining cluster [%v]", leader))
				continue
			}

			if err := cl.Close(); err != nil {
				failures = append(failures, errors.Wrapf(err, "Error closing client"))
				continue
			}

			return nil
		}
	}

	return fmt.Errorf("Unable to join cluster: %v", failures)
}

func (h *host) tryJoin(addrs []string) error {
	logger := h.replica.logger

	wait := func() error {
		timer := time.NewTimer(h.replica.Options.ElectionTimeout)
		defer timer.Stop()
		select {
		case <-h.ctrl.Closed():
			return ErrClosed
		case <-timer.C:
			return nil
		}
	}

	failures := []error{}
	for i := 0; i < 10; i++ {
		for j := 0; j < len(addrs); j++ {
			logger.Info("Attempt [%v] to join [%v]: (errs=%v)", (i+1)*(j+1)+j, addrs[j], failures)

			cl, err := Dial(h.replica.Options.Transport, addrs[j], h.replica.Options.Timeouts())
			if err != nil {
				failures = append(failures, err)
				continue
			}

			status, err := cl.Status()
			if err != nil {
				cl.Close()
				failures = append(failures, err)
				continue
			}

			// This is a race condition!!
			h.replica.Roster.Set(status.Config.Peers)
			if status.Config.Peers.Contains(h.replica.Self) {
				cl.Close()
				return nil
			}

			h.replica.Roster.Set(status.Config.Peers.Add(h.replica.Self))
			if status.Term.LeaderId == nil {
				cl.Close()

				if e := wait(); e != nil {
					return e
				}

				failures = append(failures, errors.Wrapf(ErrNoLeader, "No leader according to [%v]", status.Self))
				continue
			}

			leader, ok := status.Config.Peers[*status.Term.LeaderId]
			if !ok {
				cl.Close()

				if e := wait(); e != nil {
					return e
				}

				failures = append(failures, errors.Wrapf(ErrNoLeader, "Could not locate leader [%v]", status.Term.LeaderId))
				continue
			}

			if status.Self.Id != leader.Id {
				cl.Close()

				cl, err = leader.Dial(h.replica.Options)
				if err != nil {
					failures = append(failures, errors.Wrapf(err, "Error dialing leader [%v]", leader))
					continue
				}
			}

			if err = cl.UpdateRoster(RosterUpdateRequest{h.replica.Self, true}); err != nil {
				cl.Close()

				if errs.Is(err, ErrNotLeader) {
					if e := wait(); e != nil {
						return e
					}
				}

				failures = append(failures, errors.Wrapf(err, "Error joining cluster [%v]", leader))
				continue
			}

			return cl.Close()
		}
	}

	return fmt.Errorf("Unable to join cluster: %v", failures)
}

func (h *host) tryLeave(cancel <-chan struct{}) (err error) {
	logger := h.replica.logger

	wait := func() error {
		timer := time.NewTimer(h.replica.Options.ElectionTimeout)
		defer timer.Stop()
		select {
		case <-h.ctrl.Closed():
			return ErrClosed
		case <-cancel:
			return ErrCanceled
		case <-timer.C:
			return nil
		}
	}

	for i := 0; ; i++ {
		logger.Info("Attempting to leave cluster [attempt=%v]", i)

		leader := h.replica.Leader()
		if leader == nil {
			logger.Info("Currently there is no leader. Can't leave until one is elected")

			if err := wait(); err != nil {
				return err
			}

			continue
		}

		cl, err := leader.Dial(h.replica.Options)
		if err != nil {
			logger.Info("Error dialing leader [%v]: %v", leader, err)

			if err := wait(); err != nil {
				return err
			}

			continue
		}

		status, err := cl.Status()
		if err != nil {
			logger.Info("Error retrieving status [%v]: %v", leader, err)

			if err := wait(); err != nil {
				return err
			}

			cl.Close()
			continue
		}

		if len(status.Config.Peers) == 1 {
			cl.Close()
			return nil
		}

		if !status.Config.Peers.Contains(h.Self()) {
			cl.Close()
			return nil
		}

		if err = cl.UpdateRoster(RosterUpdateRequest{h.replica.Self, false}); err != nil {
			logger.Info("Error leaving cluster: %v", err)
			if err := wait(); err != nil {
				return err
			}

			cl.Close()
			continue
		}

		return cl.Close()
	}
}

func newLeaderPool(self *replica, size int) pool.ObjectPool {
	return pool.NewObjectPool(self.Ctx.Control(), size,
		func() (ret io.Closer, err error) {
			var cl *Client
			for cl == nil {
				leader := self.Leader()
				if leader == nil {
					timer := time.NewTimer(self.ElectionTimeout)
					select {
					case <-self.ctrl.Closed():
						timer.Stop()
						return nil, ErrClosed
					case <-timer.C:
						timer.Stop()
						continue
					}
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
	leaderPool pool.ObjectPool // T: Client
	self       *replica
}

func newLogClient(ctx context.Context, self *replica, leaderPool pool.ObjectPool) *logClient {
	ctx = ctx.Sub("LogClient")
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

func (s *logClient) Compact(cancel <-chan struct{}, until int64, data <-chan Event) error {
	return s.self.Compact(cancel, until, data)
}

func (s *logClient) Listen(start int64, buf int64) (Listener, error) {
	raw, err := s.self.Log.ListenCommits(start, buf)
	if err != nil {
		return nil, err
	}
	return newLogClientListener(raw), nil
}

func (c *logClient) Append(cancel <-chan struct{}, payload []byte) (entry Entry, err error) {
	for {
		raw, e := c.leaderPool.TakeOrCancel(cancel)
		if e != nil {
			continue
		}

		// FIXME: Implement exponential backoff
		resp, e := raw.(*Client).Append(AppendRequest{payload, Std})
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

func (s *logClient) Snapshot() (int64, EventStream, error) {
	snapshot, err := s.self.Log.Snapshot()
	if err != nil {
		return 0, nil, err
	}
	return snapshot.LastIndex(), newSnapshotStream(s.ctrl, snapshot, 1024), nil
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
