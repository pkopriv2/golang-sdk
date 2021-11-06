package raft

import (
	"sync"

	"github.com/pkopriv2/golang-sdk/lang/context"
)

// A simply notifying integer value.

type ref struct {
	val  int64
	lock *sync.Cond
	dead bool
}

func newRef(val int64) *ref {
	return &ref{val, &sync.Cond{L: &sync.Mutex{}}, false}
}

func (c *ref) WaitExceeds(cur int64) (val int64, alive bool) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	for val, alive = c.val, !c.dead; val <= cur && alive; val, alive = c.val, !c.dead {
		c.lock.Wait()
	}
	return
}

func (c *ref) WaitUntil(cur int64) (val int64, alive bool) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	for val, alive = c.val, !c.dead; val < cur && alive; val, alive = c.val, !c.dead {
		c.lock.Wait()
	}
	return
}

func (c *ref) WaitUntilOrCancel(cancel <-chan struct{}, until int64) (val int64, alive bool) {
	ctrl := context.NewControl(nil)
	defer ctrl.Close()
	go func() {
		select {
		case <-cancel:
			c.Notify()
		case <-ctrl.Closed():
		}
	}()
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	for {
		val, alive, canceled := c.val, !c.dead, context.IsClosed(cancel)
		if val >= until || !alive || canceled {
			return val, alive
		}
		c.lock.Wait()
	}
}

func (c *ref) Notify() {
	c.lock.Broadcast()
}

func (c *ref) Close() {
	c.lock.L.Lock()
	defer c.lock.Broadcast()
	defer c.lock.L.Unlock()
	c.dead = true
}

func (c *ref) Update(fn func(int64) int64) int64 {
	var cur int64
	var ret int64
	defer func() {
		if cur != ret {
			c.lock.Broadcast()
		}
	}()

	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	cur = c.val
	ret = fn(cur)
	c.val = ret
	return ret
}

func (c *ref) Set(pos int64) int64 {
	return c.Update(func(int64) int64 {
		return pos
	})
}

func (c *ref) Get() (pos int64) {
	c.lock.L.Lock()
	defer c.lock.L.Unlock()
	return c.val
}
