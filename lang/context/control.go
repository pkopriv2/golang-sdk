package context

import (
	"io"

	"github.com/pkopriv2/golang-sdk/lang/concurrent"
)

func IsClosed(c <-chan struct{}) (ok bool) {
	select {
	default:
	case <-c:
		return true
	}
	return
}

type Lifecycle Control

type Control interface {
	io.Closer
	Fail(error)
	Closed() <-chan struct{}
	IsClosed() bool
	Failure() error
	Defer(func(error))
	Sub() Control
}

type control struct {
	closes  concurrent.List
	closed  chan struct{}
	closer  chan struct{}
	failure error
}

func NewRootControl() *control {
	return NewControl(nil)
}

func NewControl(parent Control) *control {
	l := &control{
		closes: concurrent.NewList(8),
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1),
	}

	if parent != nil {
		parent.Defer(func(e error) {
			l.Fail(e)
		})
	}

	return l
}

func (c *control) Fail(cause error) {
	select {
	case <-c.closed:
		return
	case c.closer <- struct{}{}:
	}

	c.failure = cause
	for _, fn := range c.closes.All() {
		fn.(func(error))(cause)
	}

	close(c.closed)
}

func (c *control) Close() error {
	c.Fail(nil)
	return c.Failure()
}

func (c *control) Closed() <-chan struct{} {
	return c.closed
}

func (c *control) IsClosed() (ok bool) {
	select {
	default:
	case <-c.closed:
		return true
	}
	return
}

func (c *control) Failure() error {
	<-c.closed
	return c.failure
}

func (c *control) Defer(fn func(error)) {
	c.closes.Prepend(fn)
}

func (c *control) Sub() Control {
	return NewControl(c)
}
