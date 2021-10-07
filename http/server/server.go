package server

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkopriv2/golang-sdk/http/client"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/env"
	"github.com/pkopriv2/golang-sdk/lang/net"
)

func defaultListener() (net.Listener, error) {
	return net.ListenTCP4(":0")
}

type Option func(*Options)

type Options struct {
	ListenerFunc func() (net.Listener, error)
	Middleware   []Middleware
	Dependencies map[string]interface{}
}

func buildOptions(fns ...Option) (opts Options) {
	opts = Options{
		ListenerFunc: defaultListener,
		Middleware:   make([]Middleware, 0, 8),
		Dependencies: make(map[string]interface{}),
	}
	for _, fn := range fns {
		fn(&opts)
	}
	return
}

func WithListener(n net.Network, addr string) Option {
	return func(o *Options) {
		o.ListenerFunc = func() (net.Listener, error) {
			return n.Listen(addr)
		}
	}
}

func WithDependency(name string, val interface{}) Option {
	return func(o *Options) {
		o.Dependencies[name] = val
	}
}

func WithMiddleware(middleware ...Middleware) Option {
	return func(o *Options) {
		o.Middleware = append(o.Middleware, middleware...)
	}
}

type Server struct {
	env      env.Environment
	listener net.Listener
}

func Serve(ctx context.Context, builder ServiceBuilder, fns ...Option) (ret *Server, err error) {
	opts := buildOptions(fns...)

	svc, middleware :=
		buildService(builder), buildMiddleware(opts.Middleware...)

	listener, err := opts.ListenerFunc()
	if err != nil {
		return
	}

	env := env.NewEnvironment(
		ctx.Sub("Http(%v)", listener.Address().String()), env.WithDependencies(opts.Dependencies))
	env.Control().Defer(func(err error) {
		if err := listener.Close(); err != nil {
			env.Logger().Error("Unable to close listener [%+v]", err)
		}
	})

	router := mux.NewRouter()
	router.UseEncodedPath()
	for route, handler := range svc.routes {
		env.Logger().Info("Adding route [%v %v]", route.Method, route.Path)

		handler := handler
		router.Handle(route.Path,
			http.HandlerFunc(func(rawResp http.ResponseWriter, rawReq *http.Request) {
				req, resp := newRequest(rawReq), newResponseBuilder()
				defer func() {
					if err := resp.Build(rawResp); err != nil {
						env.Logger().Error("Error writing response from [%v %v]: %+v", route.Method, route.Path, err)
					}
				}()

				if fn := middleware(handler)(env, req); fn != nil {
					if err := fn(resp); err != nil {
						if err := Panic(err)(resp); err != nil {
							env.Logger().Error("Error building response from [%v %v]: %+v", route.Method, route.Path, err)
						}
					}
				} else {
					if err := StatusOK(resp); err != nil {
						env.Logger().Error("Error building response from [%v %v]: %+v", route.Method, route.Path, err)
					}
				}
			})).Methods(route.Method)
	}

	go func() {
		env.Logger().Info("Starting")
		env.Control().Fail(http.Serve(net.GoListener(listener), router))
		env.Logger().Info("Stopping")
	}()

	ret = &Server{env, listener}
	return
}

func (s *Server) Close() error {
	return s.env.Close()
}

func (s *Server) Address() net.Address {
	return s.listener.Address()
}

func (s *Server) Connect() client.Client {
	return client.NewDefaultClient(s.listener.Address().String())
}

//func adaptHandler(env env.Environment, fn Handler) http.Handler {
//return http.HandlerFunc(func(rawResp http.ResponseWriter, rawReq *http.Request) {
//req, resp := newRequest(rawReq), newResponseBuilder()
//defer func() {
//if err := resp.Build(rawResp); err != nil {
//env.Logger().Error("Error writing response from [%v %v]: %+v", route.Method, route.Path, err)
//}
//}()

//if fn := middleware(handler)(env, req); fn != nil {
//if err := fn(resp); err != nil {
//if err := Panic(err)(resp); err != nil {
//env.Logger().Error("Error building response from [%v %v]: %+v", route.Method, route.Path, err)
//}
//}
//} else {
//if err := StatusOK(resp); err != nil {
//env.Logger().Error("Error building response from [%v %v]: %+v", route.Method, route.Path, err)
//}
//}
//})
//}
