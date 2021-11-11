package rpc

import (
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/enc"
)

// Each server manages a single handler and invokes the handler for
// each request it receives.  Handler implementations must be
// thread-safe if they have access to any external resources.
type Handler func(Request) Response

// A server is basically
type Handlers map[string]Handler

type Builder func(Handlers)

func WithHandler(name string, fn Handler) Builder {
	return func(h Handlers) {
		h[name] = fn
	}
}

func BuildHandlers(b ...Builder) (ret map[string]Handler) {
	ret = make(map[string]Handler)
	for _, fn := range b {
		fn(ret)
	}
	return
}

// A server manages the resources used to serve requests in the
// background.  Clients may be spawned from the server implementation.
// This is extremely useful in testing scenarios.
type Server interface {
	io.Closer
	Connect() (Client, error)
}

// A socket accepts connections and instancies sessions from clients
type Socket interface {
	io.Closer
	Addr() string
	Accept() (Session, error)
}

// A session manages a server connection from a client.
type Session interface {
	io.Closer
	LocalAddr() string
	RemoteAddr() string
	Read(time.Duration) (Request, error)
	Send(Response, time.Duration) error
}

// A client gives consumers access to invoke a server's handlers.
type Client interface {
	io.Closer
	Send(Request) (Response, error)
}

// A simple rpc request (FUTURE: Support authentication)
type Request struct {
	Func string `json:"func"`
	Body []byte `json:"body"`
}

func (r Request) Decode(dec enc.Decoder, ptr interface{}) error {
	return dec.DecodeBinary(r.Body, ptr)
}

// Asimple rpc response
type Response struct {
	Ok   bool   `json:"ok"`
	Err  string `json:"error"`
	Body []byte `json:"body"`
}

func (r Response) Error() error {
	if !r.Ok && r.Err != "" {
		return errors.New(r.Err)
	}
	return nil
}

func (r Response) Decode(dec enc.Decoder, ptr interface{}) error {
	return dec.DecodeBinary(r.Body, ptr)
}

func BuildRequest(fns ...func(*Request) error) (ret Request, err error) {
	for _, fn := range fns {
		if err = fn(&ret); err != nil {
			return
		}
	}
	return
}

var (
	EmptyResponse = Response{Ok: true}
)

func NewErrorResponse(err error) Response {
	if err != nil {
		return Response{Err: err.Error()}
	} else {
		return Response{Ok: true}
	}
}

func NewStructResponse(enc enc.Encoder, val interface{}) (ret Response) {
	if err := enc.EncodeBinary(val, &ret.Body); err != nil {
		return NewErrorResponse(err)
	}
	ret.Ok = true
	return
}

func NewStructRequest(fn string, enc enc.Encoder, val interface{}) (ret Request, err error) {
	if err = enc.EncodeBinary(val, &ret.Body); err != nil {
		return
	}
	ret.Func = fn
	return
}
