package context

import (
	"io"
	"io/ioutil"
	"time"

	"github.com/pkopriv2/golang-sdk/lang/errs"
)

type Context interface {
	io.Closer
	Logger() Logger
	Control() Control
	Sub(fmt string, args ...interface{}) Context
}

type ctx struct {
	logger  Logger
	control Control
}

func NewContextWithLogger(log Logger) Context {
	return &ctx{logger: log, control: NewControl(nil)}
}

func NewContext(out io.Writer, lvl LogLevel) Context {
	return NewContextWithLogger(NewLogger(out, lvl, ""))
}

func NewDefaultContext() Context {
	return NewContext(ioutil.Discard, Off)
}

func (c *ctx) Close() error {
	return c.control.Close()
}

func (c *ctx) Control() Control {
	return c.control
}

func (c *ctx) Logger() Logger {
	return c.logger
}

func (c *ctx) Sub(fmt string, args ...interface{}) Context {
	return &ctx{logger: c.logger.Fmt(fmt, args...), control: c.control.Sub()}
}

// FIXME: return the sub control in order to be able to cancel/cleanup
func NewTimer(ctrl Control, dur time.Duration) Control {
	sub := NewRootControl()

	timer := time.NewTimer(dur)
	go func() {
		defer sub.Close()
		defer timer.Stop()

		select {
		case <-ctrl.Closed():
			return
		case <-sub.Closed():
			return
		case <-timer.C:
			sub.Fail(errs.TimeoutError)
			return
		}
	}()

	return sub
}
