package net

import (
	"errors"
	"io"
	"net"
	"time"
)

var (
	ClosedError = errors.New("Net:Closed")
)

// A Network is the base interface for establishing connections
type Network interface {
	Dial(timeout time.Duration, addr string) (Connection, error)
	Listen(addr string) (Listener, error)
}

// A Connection is a full-duplex streaming abstraction.
//
// Implementations are expected to be thread-safe, with
// respect to concurrent reads and writes.
type Connection interface {
	net.Conn
}

// Address represetns a network end point address.
type Address interface {
	net.Addr
}

// A simple listener abstraction.  This will be the basis of
// establishing network services
type Listener interface {
	io.Closer
	Network() Network
	Address() Address
	Connect(time.Duration) (Connection, error)
	Accept() (Connection, error)
}

func GoListener(l Listener) net.Listener {
	return adapted{l}
}

type adapted struct {
	l Listener
}

func (a adapted) Accept() (net.Conn, error) {
	return a.l.Accept()
}

func (a adapted) Close() error {
	return a.l.Close()
}

func (a adapted) Addr() net.Addr {
	return a.l.Address()
}
