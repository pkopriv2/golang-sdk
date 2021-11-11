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

func (s *socket) Accept() (ret ServerSession, err error) {
	conn, err := s.raw.Accept()
	if err != nil {
		return
	}

	ret = &serverSession{conn, s.enc}
	return
}

type serverSession struct {
	raw net.Connection
	enc enc.EncoderDecoder
}

func (s *serverSession) Close() error {
	return s.raw.Close()
}

func (s *serverSession) LocalAddr() string {
	return s.raw.LocalAddr().String()
}

func (s *serverSession) RemoteAddr() string {
	return s.raw.RemoteAddr().String()
}

func (s *serverSession) Read(timeout time.Duration) (ret Request, err error) {
	if err = s.raw.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return
	}

	p, err := readPacketRaw(s.raw)
	if err != nil {
		return
	}

	err = s.enc.DecodeBinary(p.Data, &ret)
	return
}

func (s *serverSession) Send(resp Response, timeout time.Duration) (err error) {
	buf, err := enc.Encode(s.enc, resp)
	if err != nil {
		return
	}
	if err = s.raw.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return
	}
	err = writePacketRaw(s.raw, newPacket(buf))
	return
}
