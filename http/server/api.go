package server

import (
	"io"
	"net/url"

	"github.com/pkopriv2/golang-sdk/lang/env"
)

// Each request will inherit an environment.  Contains a dependency
// injector and a context.
type Environment env.Environment

// A Handler is a function that handles a request and returns a response
type Handler func(Environment, Request) Response

// A Middleware component wraps a handler to produce another handler.  Can
// be used to inject behavior across a set of handlers.
type Middleware func(Handler) Handler

// A handler is responsible for handling a request/response
type ServiceBuilder func(*Service)

// A Service is simply a collection of handlers
type Service struct {
	routes map[Route]Handler
}

// Register a handler to the service
func (s *Service) Register(route Route, fn Handler) {
	s.routes[route] = fn
}

// Build a composite builder
func Build(fns ...ServiceBuilder) ServiceBuilder {
	return func(s *Service) {
		for _, fn := range fns {
			fn(s)
		}
	}
}

func buildService(fns ...ServiceBuilder) (ret *Service) {
	ret = &Service{make(map[Route]Handler)}
	for _, fn := range fns {
		fn(ret)
	}
	return
}

// A Route defines the abstract binding between a url and the handler
// intended to service the request.  This *should* be the primary extension
// point for adding new matching criteria.
type Route struct {
	Method string
	Path   string
}

func Get(path string) Route {
	return Route{"GET", path}
}

func Head(path string) Route {
	return Route{"HEAD", path}
}

func Post(path string) Route {
	return Route{"POST", path}
}

func Put(path string) Route {
	return Route{"PUT", path}
}

func Delete(path string) Route {
	return Route{"DELETE", path}
}

// A request encapsulates all inputs into a service handler.
type Request interface {
	io.Reader
	io.Closer
	URL() *url.URL
	Method() string
	Remote() string
	ReadHeader(string, *string) bool
	ReadPathParam(string, *string) (bool, error)
	ReadQueryParam(string, *string) (bool, error)
	ReadBody(*[]byte) error
}

// A Response is a function that updates a response builder
type Response func(ResponseBuilder) error

// A ResponseBuilder constructs an http response
type ResponseBuilder interface {
	SetCode(int)
	SetHeader(string, string)
	SetBody(mime string, val []byte)
	SetBodyRaw(mime string, r io.Reader)
}
