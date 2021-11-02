package raft

import (
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/pool"
)

type syncer struct {
	pool pool.ObjectPool // T: *rpcClient
	ref  *ref
}

func newSyncer(pool pool.ObjectPool) *syncer {
	return &syncer{pool, newRef(-1)}
}

func (s *syncer) Barrier(cancel <-chan struct{}) (int64, error) {
	for {
		val, err := s.tryBarrier(cancel)
		if err == nil || context.IsClosed(cancel) {
			return val, nil
		}
	}
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

	if context.IsClosed(cancel) {
		return ErrCanceled
	}

	return nil
}

func (s *syncer) tryBarrier(cancel <-chan struct{}) (val int64, err error) {
	raw := s.pool.TakeOrCancel(cancel)
	if raw == nil {
		return 0, ErrCanceled
	}
	defer func() {
		if err != nil {
			s.pool.Fail(raw)
		} else {
			s.pool.Return(raw)
		}
	}()
	val, err = raw.(*rpcClient).Barrier()
	return
}
