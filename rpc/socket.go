package rpc

import (
	"time"

	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/net"
)

type socket struct {
	raw net.Listener
	enc enc.EncoderDecoder
}

// Server implementation
func NewSocket(raw net.Listener, enc enc.EncoderDecoder) Socket {
	return &socket{raw, enc}
}

func (s *socket) Close() error {
	return s.raw.Close()
}

func (s *socket) Addr() string {
	return s.raw.Address().String()
}

func (s *socket) Accept() (ret Session, err error) {
	conn, err := s.raw.Accept()
	if err != nil {
		return
	}

	ret = &session{conn, s.enc}
	return
}

type session struct {
	raw net.Connection
	enc enc.EncoderDecoder
}

func (s *session) Close() error {
	return s.raw.Close()
}

func (s *session) LocalAddr() string {
	return s.raw.LocalAddr().String()
}

func (s *session) RemoteAddr() string {
	return s.raw.RemoteAddr().String()
}

func (s *session) Read(timeout time.Duration) (ret Request, err error) {
	return recvRequest(s.raw, s.enc, timeout)
}

func (s *session) Send(resp Response, timeout time.Duration) error {
	return sendResponse(s.raw, resp, s.enc, timeout)
}
