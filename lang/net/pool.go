package net

import (
	"container/list"
	"io"
	"time"

	"github.com/pkopriv2/golang-sdk/lang/errs"
)

type Dialer func(time.Duration) (Connection, error)

type ConnectionPool interface {
	io.Closer
	Max() int
	Take() Connection
	TakeTimeout(time.Duration) Connection
	Return(Connection)
	Fail(Connection)
}

type connPool struct {
	dialer  Dialer
	max     int
	timeout time.Duration
	conns   *list.List
	take    chan Connection
	ret     chan Connection
	closed  chan struct{}
	closer  chan struct{}
}

func NewConnectionPool(max int, timeout time.Duration, dial Dialer) ConnectionPool {
	p := &connPool{
		dialer:  dial,
		max:     max,
		timeout: timeout,
		conns:   list.New(),
		take:    make(chan Connection),
		ret:     make(chan Connection, max),
		closed:  make(chan struct{}),
		closer:  make(chan struct{}, 1),
	}

	p.start()
	return p
}

func (p *connPool) start() {
	go func() {
		defer p.closePool()

		out := 0

		var take chan Connection
		var next Connection
		// var err error
		for {
			take = nil
			next = nil
			if out < p.max {
				for next == nil {
					next, _ = p.takeOrSpawnFromPool()
				}
				take = p.take
			}

			select {
			case <-p.closed:
				return
			case take <- next:
				out++
			case conn := <-p.ret:
				out--
				if conn != nil {
					p.returnToPool(conn)
				}
			}
		}
	}()
}

func (p *connPool) Max() int {
	return p.max
}

func (p *connPool) Close() error {
	select {
	case <-p.closed:
		return errs.ClosedError
	case p.closer <- struct{}{}:
	}

	close(p.closed)
	return nil
}

func (p *connPool) Take() Connection {
	select {
	case <-p.closed:
		return nil
	case conn := <-p.take:
		return conn
	}
}

func (p *connPool) TakeTimeout(dur time.Duration) (conn Connection) {
	timer := time.NewTimer(dur)
	select {
	case <-timer.C:
		return nil
	case <-p.closed:
		return nil
	case conn := <-p.take:
		return conn
	}
}

func (p *connPool) Fail(c Connection) {
	defer c.Close()
	select {
	case <-p.closed:
	case p.ret <- nil:
	}
}

func (p *connPool) Return(c Connection) {
	select {
	case <-p.closed:
	case p.ret <- c:
	}
}

func (p *connPool) closePool() (err error) {
	for item := p.conns.Front(); item != nil; item = p.conns.Front() {
		item.Value.(io.Closer).Close()
	}
	return
}

func (p *connPool) spawn() (Connection, error) {
	return p.dialer(p.timeout)
}

func (p *connPool) returnToPool(c Connection) {
	p.conns.PushFront(c)
}

func (p *connPool) takeOrSpawnFromPool() (Connection, error) {
	if item := p.conns.Front(); item != nil {
		p.conns.Remove(item)
		return item.Value.(Connection), nil
	}

	return p.spawn()
}
