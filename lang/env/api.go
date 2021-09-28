package env

import (
	"io"

	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/dep"
)

type Environment interface {
	io.Closer
	Context() context.Context
	Control() context.Control
	Logger() context.Logger
	Register(name string, val interface{})
	Assign(name string, ptr interface{})
}

type Option func(Environment)

func WithDependency(name string, val interface{}) Option {
	return func(e Environment) {
		e.Register(name, val)
	}
}

func WithDependencies(deps map[string]interface{}) Option {
	return func(e Environment) {
		for name, val := range deps {
			e.Register(name, val)
		}
	}
}

type env struct {
	ctx context.Context
	dep *dep.Injector
}

func NewEnvironment(ctx context.Context, fns ...Option) (ret Environment) {
	ret = &env{ctx.Sub("Env"), dep.NewInjector()}
	for _, fn := range fns {
		fn(ret)
	}
	return
}

func (e *env) Close() error {
	return e.Context().Close()
}

func (e *env) Context() context.Context {
	return e.ctx
}

func (e *env) Control() context.Control {
	return e.ctx.Control()
}

func (e *env) Logger() context.Logger {
	return e.ctx.Logger()
}

func (e *env) Register(name string, val interface{}) {
	e.dep.Register(name, val)
}

func (e *env) Assign(name string, ptr interface{}) {
	e.dep.Assign(name, ptr)
}
