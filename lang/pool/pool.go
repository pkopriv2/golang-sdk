package pool

import (
	"container/list"
	"io"
	"time"

	"github.com/pkopriv2/golang-sdk/lang/chans"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/errs"
)

type ObjectPool interface {
	io.Closer
	Max() int
	Take() io.Closer
	TakeTimeout(time.Duration) io.Closer
	TakeOrCancel(<-chan struct{}) io.Closer
	Return(io.Closer)
	Fail(io.Closer)
}

type objectPool struct {
	ctrl context.Control
	fn   func() (io.Closer, error)
	raw  *list.List
	max  int
	take chan *chans.Request
	ret  chan io.Closer
}

func NewObjectPool(ctrl context.Control, max int, fn func() (io.Closer, error)) ObjectPool {
	p := &objectPool{
		ctrl: ctrl.Sub(),
		fn:   fn,
		max:  max,
		raw:  list.New(),
		take: make(chan *chans.Request),
		ret:  make(chan io.Closer, max),
	}
	ctrl.Defer(func(error) {
		p.closePool()
	})

	p.start()
	return p
}

func (p *objectPool) start() {
	go func() {
		var take chan *chans.Request
		var next io.Closer
		for out := 0; ; {
			take = nil
			if out < p.max {
				take = p.take
			}

			select {
			case <-p.ctrl.Closed():
				if next != nil {
					next.Close()
				}
				return
			case obj := <-p.ret:
				out--
				if obj != nil {
					p.returnToPool(obj)
					continue
				}
			case req := <-take:
				out++
				req.Return(p.takeOrSpawnFromPool())
			}
		}
	}()
}

func (p *objectPool) Max() int {
	return p.max
}

func (p *objectPool) Close() error {
	return p.ctrl.Close()
}

func (p *objectPool) Take() io.Closer {
	if raw, err := chans.SendRequest(p.ctrl, p.take, p.ctrl.Closed(), nil); err == nil {
		return raw.(io.Closer)
	}
	return nil
}

func (p *objectPool) TakeOrCancel(cancel <-chan struct{}) (conn io.Closer) {
	if raw, err := chans.SendRequest(p.ctrl, p.take, cancel, nil); err == nil {
		return raw.(io.Closer)
	}
	return nil
}

func (p *objectPool) TakeTimeout(dur time.Duration) (conn io.Closer) {
	timer := context.NewTimer(p.ctrl, dur)
	defer timer.Close()
	if raw, err := chans.SendRequest(p.ctrl, p.take, timer.Closed(), nil); err == nil {
		return raw.(io.Closer)
	}
	return nil
}

func (p *objectPool) Fail(c io.Closer) {
	c.Close()
	select {
	case <-p.ctrl.Closed():
	case p.ret <- nil:
	}
}

func (p *objectPool) Return(c io.Closer) {
	select {
	case <-p.ctrl.Closed():
	case p.ret <- c:
	}
}

func (p *objectPool) spawn() (io.Closer, error) {
	return p.fn()
}

func (p *objectPool) closePool() (err error) {
	for item := p.raw.Front(); item != nil; item = p.raw.Front() {
		val := p.raw.Remove(item)
		err = errs.Or(err, val.(io.Closer).Close())
	}
	return
}

func (p *objectPool) returnToPool(c io.Closer) {
	p.raw.PushFront(c)
}

func (p *objectPool) takeOrSpawnFromPool() (io.Closer, error) {
	if item := p.raw.Front(); item != nil {
		val := p.raw.Remove(item)
		return val.(io.Closer), nil
	}

	return p.spawn()
}
