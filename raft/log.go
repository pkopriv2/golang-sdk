package raft

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/context"
)

// NOTE: With regard to error handling, there are specific error causes that consumers
// will be looking for.  Therefore, do NOT decorate any errors that are the result
// of the StoredLog operation.

type log struct {
	ctx     context.Context
	ctrl    context.Control
	logger  context.Logger
	storage StoredLog
	head    *ref
	commit  *ref
}

func openLog(ctx context.Context, storage StoredLog) (*log, error) {
	head, _, err := storage.LastIndexAndTerm()
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to retrieve head position for segment [%v]", storage.Id())
	}

	ctx = ctx.Sub("Log")

	headRef := newRef(head)
	ctx.Control().Defer(func(error) {
		headRef.Close()
	})

	commitRef := newRef(-1)
	ctx.Control().Defer(func(error) {
		commitRef.Close()
	})

	return &log{
		ctx:     ctx,
		ctrl:    ctx.Control(),
		logger:  ctx.Logger(),
		storage: storage,
		head:    headRef,
		commit:  commitRef}, nil
}

func (e *log) Close() error {
	return e.ctrl.Close()
}

func (e *log) Store() LogStore {
	return e.storage.Store()
}

func (e *log) Head() int64 {
	return e.head.Get()
}

func (e *log) Committed() (pos int64) {
	return e.commit.Get()
}

func (e *log) Commit(pos int64) (actual int64, err error) {
	var head int64
	actual = e.commit.Update(func(cur int64) int64 {
		head, _, err = e.storage.LastIndexAndTerm() // does this need to be in critical section?
		if err != nil {
			return cur
		}

		return max(cur, min(pos, head))
	})
	return
}

func (e *log) Get(index int64) (Entry, bool, error) {
	return e.storage.Get(index)
}

func (e *log) Scan(start int64, end int64) ([]Entry, error) {
	return e.storage.Scan(start, end)
}

func (e *log) Append(payload []byte, term int64, k Kind) (ret Entry, err error) {
	ret, err = e.storage.Append(payload, term, k)
	if err != nil {
		return
	}

	e.head.Update(func(cur int64) int64 {
		return max(cur, ret.Index)
	})
	return
}

func (e *log) Insert(batch []Entry) (err error) {
	if len(batch) == 0 {
		return
	}

	if err = e.storage.Insert(batch); err != nil {
		return
	}

	e.head.Update(func(cur int64) int64 {
		return max(cur, batch[len(batch)-1].Index)
	})
	return
}

func (e *log) Snapshot() (StoredSnapshot, error) {
	return e.storage.Snapshot()
}

// only called from followers.
func (e *log) Assert(index int64, term int64) (ok bool, err error) {
	item, found, err := e.Get(index)
	if err != nil {
		return
	}

	if found {
		ok = item.Term == term
		return
	}

	s, err := e.Snapshot()
	if err != nil {
		return
	}

	ok = s.LastIndex() == index && s.LastTerm() == term
	return
}

func (e *log) NewSnapshot(cancel <-chan struct{}, lastIndex int64, lastTerm int64, data <-chan Event, config Config) (StoredSnapshot, error) {
	return NewSnapshot(e.storage.Store(), lastIndex, lastTerm, config, data, cancel)
}

func (e *log) Install(snapshot StoredSnapshot) (err error) {
	if err = e.storage.Install(snapshot); err != nil {
		return
	}

	e.head.Update(func(cur int64) int64 {
		return max(cur, snapshot.LastIndex())
	})
	return
}

func (e *log) Compact(cancel <-chan struct{}, until int64, data <-chan Event, config Config) (err error) {
	item, ok, err := e.Get(until)
	if err != nil || !ok {
		return errors.Wrapf(ErrCompaction, "Cannot compact until [%v].  It doesn't exist", until)
	}

	snapshot, err := e.NewSnapshot(cancel, item.Index, item.Term, data, config)
	if err != nil {
		return
	}

	return e.storage.Install(snapshot)
}

func (e *log) LastIndexAndTerm() (int64, int64, error) {
	return e.storage.LastIndexAndTerm()
}

func (e *log) ListenCommits(start int64, buf int64) (Listener, error) {
	if e.ctrl.IsClosed() {
		return nil, ErrClosed
	}

	return newRefListener(e, e.commit, start, buf), nil
}

func (e *log) ListenAppends(start int64, buf int64) (Listener, error) {
	if e.ctrl.IsClosed() {
		return nil, ErrClosed
	}

	return newRefListener(e, e.head, start, buf), nil
}

type refListener struct {
	log    *log
	pos    *ref
	buf    int64
	ch     chan Entry
	ctrl   context.Control
	logger context.Logger
}

func newRefListener(log *log, pos *ref, from int64, buf int64) *refListener {
	ctx := log.ctx.Sub("Listener")

	l := &refListener{
		log:    log,
		pos:    pos,
		buf:    buf,
		ch:     make(chan Entry, buf),
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
	}
	l.start(from)
	return l
}

func (l *refListener) start(from int64) {
	go func() {
		defer l.Close()

		next := from
		for {
			horizon, ok := l.pos.WaitUntil(next)
			if !ok || l.ctrl.IsClosed() || l.log.ctrl.IsClosed() {
				return
			}

			// FIXME: Can still miss truncations
			if horizon < next {
				l.ctrl.Fail(errors.Wrapf(ErrOutOfBounds, "Log truncated to [%v] was [%v]", next, next))
				return
			}

			for next <= horizon {
				if l.ctrl.IsClosed() || l.log.ctrl.IsClosed() {
					return
				}

				// scan the next batch
				batch, err := l.log.Scan(next, min(horizon+1, next+1+l.buf))
				if err != nil {
					l.ctrl.Fail(err)
					return
				}

				// start emitting
				for _, i := range batch {
					select {
					case <-l.log.ctrl.Closed():
						return
					case <-l.ctrl.Closed():
						return
					case l.ch <- i:
					}
				}

				//update current
				next = next + int64(len(batch))
			}
		}
	}()
}

func (p *refListener) Data() <-chan Entry {
	return p.ch
}

func (p *refListener) Ctrl() context.Control {
	return p.ctrl
}

func (l *refListener) Close() error {
	return l.ctrl.Close()
}
