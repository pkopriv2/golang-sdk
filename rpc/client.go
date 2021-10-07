package rpc

import (
	"time"

	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/errs"
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

func WithListener(n net.Network, addr string) Option {
	return func(o *Options) {
		o.Listener = func() (net.Listener, error) {
			return n.Listen(addr)
		}
	}
}

func buildOptions(fns []Option) (ret Options) {
	ret = Options{
		DefaultDialTimeout,
		DefaultReadTimeout,
		DefaultSendTimeout,
		DefaultListener,
		enc.Gob,
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
	ctx    context.Context
	ctrl   context.Control
	log    context.Logger
	dialer net.Dialer
	out    chan request
	opts   Options
}

func NewClient(ctx context.Context, dialer net.Dialer, o ...Option) Client {
	ctx = ctx.Sub("RpcClient")
	return &client{
		ctx:    ctx,
		ctrl:   ctx.Control(),
		log:    ctx.Logger(),
		dialer: dialer,
		opts:   buildOptions(o)}
}

func (s *client) Close() error {
	return s.ctrl.Close()
}

func (s *client) Send(cancel <-chan struct{}, req Request) (res Response, err error) {
	resp := make(chan Response, 1)
	select {
	case <-s.ctrl.Closed():
		err = s.ctrl.Failure()
		return
	case <-cancel:
		err = errs.CanceledError
		return
	case s.out <- request{req, resp}:
	}

	select {
	case <-s.ctrl.Closed():
		err = s.ctrl.Failure()
		return
	case <-cancel:
		err = errs.CanceledError
		return
	case res = <-resp:
	}
	return
}

func (c *client) start() {

	// The main connection manager.  This routine is responsible
	// for maintaining a healthy connection to the remote endpoint.
	// Only a single instance of this routine may be run at a
	// given time.
	go func() {
		defer c.ctx.Close()

		respond := func(req request, resp Response) {
			select {
			case <-c.ctrl.Closed():
			case req.Resp <- resp:
			}
		}

		for !c.ctrl.IsClosed() {
			conn, err := c.reconnect()
			if err != nil {
				c.ctrl.Fail(err)
				return
			}

			var req request
			select {
			case <-c.ctrl.Closed():
				conn.Close()
				return
			case req = <-c.out:
			}

			if err := sendRequest(conn, req.Raw, c.opts); err != nil {
				respond(req, Response{Error: err})
				conn.Close()
				continue
			}

			resp, err := recvResponse(conn, c.opts)
			if err != nil {
				respond(req, Response{Error: err})
				conn.Close()
				continue
			}

			respond(req, resp)
		}
	}()
	return
}

func (c *client) reconnect() (ret net.Connection, err error) {
	c.log.Info("Reconnecting")

	// Uses exponential backoff to reduce congestion
	sleep := 1 * time.Second
	for {

		if ret, err = c.dialer(c.opts.DialTimeout); err == nil {
			c.log.Info("Connected")
			return
		}

		c.log.Error("Error dialing router [%v]", err)

		timer := time.After(sleep)
		select {
		case <-c.ctrl.Closed():
			err = errs.Or(c.ctrl.Failure(), errs.ClosedError)
			return
		case <-timer:
		}

		if sleep < 30*time.Second {
			sleep *= 2
		}

		c.log.Info("Attempting retry in [%v]", sleep)
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
	var p packet
	if err = readPacketRaw(conn, &p); err != nil {
		return
	}

	err = opts.Encoder.DecodeBinary(p.Data, &resp)
	return
}
