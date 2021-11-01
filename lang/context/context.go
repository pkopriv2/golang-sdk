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

func NewContext(out io.Writer, lvl LogLevel) Context {
	return &ctx{logger: NewLogger(out, lvl, ""), control: NewControl(nil)}
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
	sub := ctrl.Sub()

	timer := time.NewTimer(dur)
	go func() {
		defer sub.Close()

		select {
		case <-sub.Closed():
			return
		case <-timer.C:
			sub.Fail(errs.TimeoutError)
			return
		}
	}()

	return sub
}
