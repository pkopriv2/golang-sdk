package rpc

import (
	"time"

	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/net"
)

const (
	DefaultDialTimeout = 120 * time.Second
	DefaultReadTimeout = 30 * time.Second
	DefaultSendTimeout = 30 * time.Second
	DefaultWorkers     = 10
)

var DefaultListener = func() (net.Listener, error) {
	return net.ListenTCP4(":0")
}

var NewStandardDialer = net.NewStandardDialer

func NewDialer(n net.Network, addr string) net.Dialer {
	return func(timeout time.Duration) (net.Connection, error) {
		return n.Dial(timeout, addr)
	}
}

type Options struct {
	DialTimeout time.Duration
	ReadTimeout time.Duration
	SendTimeout time.Duration
	Listener    func() (net.Listener, error)
	Encoder     enc.EncoderDecoder
	NumWorkers  int
}

type Option func(*Options)

func WithDialTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.DialTimeout = timeout
	}
}

func WithReadTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.ReadTimeout = timeout
	}
}

func WithSendTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.SendTimeout = timeout
	}
}

func WithListener(l net.Listener) Option {
	return func(o *Options) {
		o.Listener = func() (net.Listener, error) {
			return l, nil
		}
	}
}

func WithEncoder(e enc.EncoderDecoder) Option {
	return func(o *Options) {
		o.Encoder = e
	}
}

func WithNumWorkers(num int) Option {
	return func(o *Options) {
		o.NumWorkers = num
	}
}

func buildOptions(fns []Option) (ret Options) {
	ret = Options{
		DefaultDialTimeout,
		DefaultReadTimeout,
		DefaultSendTimeout,
		DefaultListener,
		enc.Json,
		DefaultWorkers,
	}
	for _, fn := range fns {
		fn(&ret)
	}
	return
}

type clientSession struct {
	raw net.Connection
	enc enc.EncoderDecoder
}

func Dial(dialer net.Dialer, o ...Option) (ret ClientSession, err error) {
	opts := buildOptions(o)

	conn, err := dialer(opts.DialTimeout)
	if err != nil {
		return
	}

	ret = &clientSession{conn, opts.Encoder}
	return
}

func (s *clientSession) Close() error {
	return s.raw.Close()
}

func (s *clientSession) LocalAddr() string {
	return s.raw.LocalAddr().String()
}

func (s *clientSession) RemoteAddr() string {
	return s.raw.RemoteAddr().String()
}

func (s *clientSession) Read(timeout time.Duration) (ret Response, err error) {
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

func (s *clientSession) Send(req Request, timeout time.Duration) (err error) {
	buf, err := enc.Encode(s.enc, req)
	if err != nil {
		return
	}
	if err = s.raw.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return
	}
	err = writePacketRaw(s.raw, newPacket(buf))
	return
}

func NewClient(conn ClientSession, o ...Option) (ret Client) {
	return &client{conn, buildOptions(o)}
}

type client struct {
	conn ClientSession
	opts Options
}

func (c *client) Close() error {
	return c.conn.Close()
}

func (c *client) Send(req Request) (res Response, err error) {
	if err = c.conn.Send(req, c.opts.SendTimeout); err != nil {
		c.Close()
		return
	}

	if res, err = c.conn.Read(c.opts.ReadTimeout); err != nil {
		c.Close()
		return
	}
	return
}
