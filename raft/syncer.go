package raft

import (
	"io"
	"time"

	"github.com/pkopriv2/golang-sdk/lang/pool"
)

type syncer struct {
	pool pool.ObjectPool // T: *Client
	ref  *ref
}

func newSyncer(pool pool.ObjectPool) *syncer {
	return &syncer{pool, newRef(-1)}
}

func (s *syncer) Close() (err error) {
	s.ref.Close()
	return
}

func (s *syncer) Ack(index int64) {
	s.ref.Update(func(cur int64) int64 {
		return max(cur, index)
	})
}

func (s *syncer) Sync(cancel <-chan struct{}, index int64) error {
	_, alive := s.ref.WaitUntilOrCancel(cancel, index)
	if !alive {
		return ErrClosed
	}

	return nil
}

func (s *syncer) Barrier(cancel <-chan struct{}) (val int64, err error) {
	wait := 50 * time.Millisecond
	for {
		if wait < 30*time.Second {
			wait = wait * 2
		}

		sleep := func() (err error) {
			timer := time.NewTimer(wait)
			defer timer.Stop()
			select {
			case <-cancel:
				return ErrCanceled
			case <-timer.C:
				return
			}
		}

		var raw io.Closer
		raw, err = s.pool.TakeOrCancel(cancel)
		if err != nil {
			return
		}

		var resp ReadBarrierResponse
		resp, err = raw.(*Client).Barrier()
		if err != nil {
			s.pool.Fail(raw)

			if err := sleep(); err != nil {
				return -1, err
			}

			continue
		}

		val = resp.Barrier
		s.pool.Return(raw)
		return
	}
}
