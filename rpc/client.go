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

type request struct {
	Raw  Request
	Resp chan<- Response
}

type client struct {
	conn net.Connection
	opts Options
}

func Dial(dialer net.Dialer, o ...Option) (ret Client, err error) {
	opts := buildOptions(o)

	conn, err := dialer(opts.DialTimeout)
	if err != nil {
		return
	}

	ret = &client{conn: conn, opts: opts}
	return
}

func (c *client) Close() error {
	return c.conn.Close()
}

func (c *client) Send(req Request) (res Response, err error) {
	if err = sendRequest(c.conn, req, c.opts); err != nil {
		c.Close()
		return
	}

	if res, err = recvResponse(c.conn, c.opts); err != nil {
		c.Close()
		return
	}
	return
}

func sendRequest(conn net.Connection, req Request, opts Options) (err error) {
	var buf []byte
	if err = opts.Encoder.EncodeBinary(req, &buf); err != nil {
		return
	}
	if err = conn.SetWriteDeadline(time.Now().Add(opts.SendTimeout)); err != nil {
		return
	}
	err = writePacketRaw(conn, newPacket(buf))
	return
}

func recvResponse(conn net.Connection, opts Options) (resp Response, err error) {
	if err = conn.SetReadDeadline(time.Now().Add(opts.ReadTimeout)); err != nil {
		return
	}

	p, err := readPacketRaw(conn)
	if err != nil {
		return
	}

	err = opts.Encoder.DecodeBinary(p.Data, &resp)
	return
}
