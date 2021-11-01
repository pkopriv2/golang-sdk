package rpc

import (
	"io"

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
	Ok    bool   `json:"ok"`
	Error error  `json:"error"`
	Body  []byte `json:"body"`
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

func NewErrorResponse(err error) Response {
	return Response{Error: err}
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
