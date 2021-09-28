package net

import (
	"net"
	"time"
)

func ListenTCP4(addr string) (Listener, error) {
	l, err := net.Listen("tcp4", addr)
	if err != nil {
		return nil, err
	}

	return &TCPListener{l, &TCP4Network{}}, nil
}

func DialTCP4(timeout time.Duration, addr string) (Connection, error) {
	conn, err := net.DialTimeout("tcp4", addr, timeout)
	if err != nil {
		return nil, err
	}

	return &TCPConnection{conn}, nil
}

type TCP4Network struct{}

func (t *TCP4Network) Dial(timeout time.Duration, addr string) (Connection, error) {
	return DialTCP4(timeout, addr)
}

func (t *TCP4Network) Listen(addr string) (Listener, error) {
	return ListenTCP4(addr)
}

type TCPListener struct {
	raw net.Listener
	net Network
}

func (t *TCPListener) Close() error {
	return t.raw.Close()
}

func (t *TCPListener) Network() Network {
	return t.net
}

func (t *TCPListener) Address() Address {
	return t.raw.Addr()
}

func (t *TCPListener) Connect(timeout time.Duration) (Connection, error) {
	return DialTCP4(timeout, t.raw.Addr().String())
}

func (t *TCPListener) Accept() (Connection, error) {
	conn, err := t.raw.Accept()
	if err != nil {
		return nil, err
	}

	return &TCPConnection{conn}, nil
}

type TCPConnection struct {
	net.Conn
}
