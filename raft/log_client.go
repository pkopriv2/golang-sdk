package raft

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkopriv2/golang-sdk/lang/pool"
	uuid "github.com/satori/go.uuid"
)

// This is the public facing client.  Only emits committed items.
type logClient struct {
	id     uuid.UUID
	ctx    context.Context
	ctrl   context.Control
	logger context.Logger
	pool   pool.ObjectPool // T: *rpcClient
	self   *replica
}

func newLogClient(self *replica, pool pool.ObjectPool) *logClient {
	ctx := self.Ctx.Sub("LogClient")
	return &logClient{
		id:     self.Self.Id,
		ctx:    ctx,
		logger: ctx.Logger(),
		ctrl:   ctx.Control(),
		self:   self,
		pool:   pool,
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
	raw := c.pool.TakeOrCancel(cancel)
	if raw == nil {
		return Entry{}, errors.WithStack(errs.CanceledError)
	}
	defer func() {
		if err != nil {
			c.pool.Fail(raw)
		} else {
			c.pool.Return(raw)
		}
	}()
	resp, err := raw.(*rpcClient).Append(appendEventRequest{payload, kind})
	if err != nil {
		return Entry{}, err
	}

	return Entry{
		Kind:    Std,
		Term:    resp.Term,
		Index:   resp.Index,
		Payload: payload,
	}, nil
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
