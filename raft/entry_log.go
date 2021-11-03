package raft

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/context"
)

// NOTE: With regard to error handling, there are specific error causes that consumers
// will be looking for.  Therefore, do NOT decorate any errors that are the result
// of the StoredLog operation.

type entryLog struct {
	ctx    context.Context
	ctrl   context.Control
	logger context.Logger
	raw    StoredLog
	head   *ref
	commit *ref
}

func openEntryLog(ctx context.Context, log StoredLog) (*entryLog, error) {
	head, _, err := log.LastIndexAndTerm()
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to retrieve head position for segment [%v]", log.Id())
	}

	ctx = ctx.Sub("EventLog")

	headRef := newRef(head)
	ctx.Control().Defer(func(error) {
		ctx.Logger().Info("Closing head ref")
		headRef.Close()
	})

	commitRef := newRef(-1)
	ctx.Control().Defer(func(error) {
		ctx.Logger().Info("Closing commit ref")
		commitRef.Close()
	})

	return &entryLog{
		ctx:    ctx,
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
		raw:    log,
		head:   headRef,
		commit: commitRef}, nil
}

func (e *entryLog) Close() error {
	return e.ctrl.Close()
}

func (e *entryLog) Head() int64 {
	return e.head.Get()
}

func (e *entryLog) Committed() (pos int64) {
	return e.commit.Get()
}

func (e *entryLog) Commit(pos int64) (actual int64, err error) {
	var head int64
	actual = e.commit.Update(func(cur int64) int64 {
		head, _, err = e.raw.LastIndexAndTerm()
		if err != nil {
			return cur
		}
		return max(cur, min(pos, head))
	})
	return
}

func (e *entryLog) Get(index int64) (Entry, bool, error) {
	return e.raw.Get(index)
}

func (e *entryLog) Scan(start int64, end int64) ([]Entry, error) {
	return e.raw.Scan(start, end)
}

func (e *entryLog) Append(payload []byte, term int64, k Kind) (Entry, error) {
	item, err := e.raw.Append(payload, term, k)
	if err != nil {
		return item, err
	}

	e.head.Update(func(cur int64) int64 {
		return max(cur, item.Index)
	})

	return item, nil
}

func (e *entryLog) Insert(batch []Entry) error {
	if len(batch) == 0 {
		return nil
	}

	if err := e.raw.Insert(batch); err != nil {
		return err
	}

	e.head.Update(func(cur int64) int64 {
		return max(cur, batch[len(batch)-1].Index)
	})
	return nil
}

func (e *entryLog) TrimRight(start int64) (err error) {
	e.head.Update(func(cur int64) int64 {
		if start > cur {
			return cur
		}

		err = e.raw.TrimRight(start)
		if err != nil {
			return cur
		} else {
			return start - 1
		}
	})
	return
}

func (e *entryLog) Snapshot() (StoredSnapshot, error) {
	return e.raw.Snapshot()
}

// only called from followers.
func (e *entryLog) Assert(index int64, term int64) (bool, error) {
	item, ok, err := e.Get(index)
	if err != nil {
		return false, err
	}

	if ok {
		return item.Term == term, nil
	}

	s, err := e.Snapshot()
	if err != nil {
		return false, err
	}

	return s.LastIndex() == index && s.LastTerm() == term, nil
}

func (e *entryLog) NewSnapshot(lastIndex int64, lastTerm int64, ch <-chan Event, config Config) (StoredSnapshot, error) {
	return e.raw.Store().NewSnapshot(lastIndex, lastTerm, ch, config)
}

func (e *entryLog) Install(snapshot StoredSnapshot) error {
	err := e.raw.Install(snapshot)
	if err != nil {
		return err
	}

	last := snapshot.LastIndex()
	e.head.Update(func(cur int64) int64 {
		if cur < last {
			return last
		} else {
			return cur
		}
	})

	return err
}

func (e *entryLog) Compact(until int64, data <-chan Event, config Config) error {
	item, ok, err := e.Get(until)
	if err != nil || !ok {
		return errors.Wrapf(ErrCompaction, "Cannot compact until [%v].  It doesn't exist", until)
	}

	snapshot, err := e.NewSnapshot(item.Index, item.Term, data, config)
	if err != nil {
		return err
	}

	return e.raw.Install(snapshot)
}

func (e *entryLog) LastIndexAndTerm() (int64, int64, error) {
	return e.raw.LastIndexAndTerm()
}

func (e *entryLog) ListenCommits(start int64, buf int64) (Listener, error) {
	if e.ctrl.IsClosed() {
		return nil, errors.WithStack(ErrClosed)
	}

	return newRefListener(e, e.commit, start, buf), nil
}

func (e *entryLog) ListenAppends(start int64, buf int64) (Listener, error) {
	if e.ctrl.IsClosed() {
		return nil, errors.WithStack(ErrClosed)
	}

	return newRefListener(e, e.head, start, buf), nil
}

type snapshot struct {
	raw       StoredSnapshot
	PrevIndex int64
	PrevTerm  int64
}

func (s *snapshot) Size() int64 {
	return s.raw.Size()
}

func (s *snapshot) Config() Config {
	return s.raw.Config()
}

func (s *snapshot) Events(cancel <-chan struct{}) <-chan Event {
	ch := make(chan Event)
	go func() {
		size := s.raw.Size()
		for cur := int64(0); cur < size; {
			batch, err := s.raw.Scan(cur, min(size+1, cur+256))
			if err != nil {
				return
			}

			for _, e := range batch {
				select {
				case ch <- e:
				case <-cancel:
					return
				}
			}

			cur = cur + int64(len(batch))
		}

		close(ch)
	}()
	return ch
}

type refListener struct {
	log    *entryLog
	pos    *ref
	buf    int64
	ch     chan Entry
	ctrl   context.Control
	logger context.Logger
}

func newRefListener(log *entryLog, pos *ref, from int64, buf int64) *refListener {
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

		cur := from
		for {
			next, ok := l.pos.WaitUntil(cur)
			if !ok || l.ctrl.IsClosed() || l.log.ctrl.IsClosed() {
				return
			}

			// FIXME: Can still miss truncations
			if next < cur {
				l.ctrl.Fail(errors.Wrapf(ErrOutOfBounds, "Log truncated to [%v] was [%v]", next, cur))
				return
			}

			for cur < next {
				if l.ctrl.IsClosed() || l.log.ctrl.IsClosed() {
					return
				}

				// scan the next batch
				batch, err := l.log.Scan(cur, min(next+1, cur+l.buf))
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

				// update current
				cur = cur + int64(len(batch))
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