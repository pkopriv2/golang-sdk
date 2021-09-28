package context

import (
	"io"
	"io/ioutil"
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
