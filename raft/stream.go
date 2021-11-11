package raft

import "github.com/pkopriv2/golang-sdk/lang/context"

type snapshotStream struct {
	ctrl     context.Control
	snapshot DurableSnapshot
	buf      int64
	ch       chan Event
}

func newSnapshotStream(ctrl context.Control, snapshot DurableSnapshot, buf int64) *snapshotStream {
	l := &snapshotStream{
		ctrl:     ctrl.Sub(),
		snapshot: snapshot,
		buf:      buf,
		ch:       make(chan Event, buf),
	}
	l.start()
	return l
}

func (l *snapshotStream) start() {
	go func() {
		defer l.Close()

		for i := int64(0); i < l.snapshot.Size(); {
			if l.ctrl.IsClosed() {
				return
			}

			// scan the next batch
			batch, err := l.snapshot.Scan(i, min(i+1+l.buf, l.snapshot.Size()))
			if err != nil {
				l.ctrl.Fail(err)
				return
			}

			// start emitting
			for _, e := range batch {
				select {
				case <-l.ctrl.Closed():
					return
				case l.ch <- e:
				}
			}

			// update current
			i = i + int64(len(batch))
		}
	}()
}

func (l *snapshotStream) Close() error {
	return l.ctrl.Close()
}

func (l *snapshotStream) Ctrl() context.Control {
	return l.ctrl
}

func (l *snapshotStream) Data() <-chan Event {
	return l.ch
}
