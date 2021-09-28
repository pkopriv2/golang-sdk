package net

import (
	"crypto/tls"
	"time"

	"github.com/pkg/errors"
)

func NewTLSNetwork() *TLSNetwork {
	return NewTLSNetworkWithConfig(&tls.Config{})
}

func NewTLSNetworkWithConfig(c *tls.Config) *TLSNetwork {
	return &TLSNetwork{c}
}

func DialTLS(timeout time.Duration, addr string, c *tls.Config) (Connection, error) {
	conn, err := tls.Dial("tcp", addr, c)
	if err != nil {
		return nil, err
	}

	if conn == nil {
		return nil, errors.Errorf("Error opening connection [%v]", addr)
	}

	return &TCPConnection{conn}, nil
}

func ListenTLS(addr string, c *tls.Config) (Listener, error) {
	listen, err := tls.Listen("tcp", addr, c)
	if err != nil {
		return nil, err
	}

	return &TCPListener{listen, &TLSNetwork{}}, nil
}

type TLSNetwork struct {
	config *tls.Config
}

func (t *TLSNetwork) Dial(timeout time.Duration, addr string) (Connection, error) {
	return DialTLS(timeout, addr, t.config)
}

func (t *TLSNetwork) Listen(addr string) (Listener, error) {
	return ListenTLS(addr, t.config)
}
