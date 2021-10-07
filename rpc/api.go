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
	Send(<-chan struct{}, Request) (Response, error)
}

// A simple rpc request (FUTURE: Support authentication)
type Request struct {
	Func string `json:"func"`
	Body []byte `json:"body"`
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
