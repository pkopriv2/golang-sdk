package rpc

import (
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkopriv2/golang-sdk/lang/net"
)

var (
	ErrNoHandler = fmt.Errorf("Rpc:NoHandler")
)

// Server implementation
func Serve(ctx context.Context, funcs Handlers, o ...Option) (ret Server, err error) {
	opts := buildOptions(o)

	listener, err := opts.Listener()
	if err != nil {
		return
	}

	ctx = ctx.Sub("RpcServer(%v)", listener.Address().String())
	ctx.Control().Defer(func(error) {
		listener.Close()
	})

	pool := NewWorkPool(ctx.Control(), opts.NumWorkers)
	ctx.Control().Defer(func(error) {
		pool.Close()
	})

	ctx.Control().Defer(func(cause error) {
		ctx.Logger().Info("Shutting down server: %+v", cause)
	})

	s := &server{
		context:  ctx,
		ctrl:     ctx.Control(),
		logger:   ctx.Logger(),
		listener: listener,
		funcs:    funcs,
		pool:     pool,
		opts:     opts}

	s.start()
	return s, nil
}

type server struct {
	context  context.Context
	logger   context.Logger
	ctrl     context.Control
	pool     WorkPool
	funcs    Handlers
	listener net.Listener
	opts     Options
}

func (s *server) Connect() (ret Client, err error) {
	if s.ctrl.IsClosed() {
		err = errs.ClosedError
		return
	}

	dialer := func(t time.Duration) (net.Connection, error) {
		return s.listener.Connect(t)
	}

	return Dial(dialer, func(o *Options) {
		o.ReadTimeout = s.opts.ReadTimeout
		o.DialTimeout = s.opts.DialTimeout
		o.SendTimeout = s.opts.SendTimeout
		o.Encoder = s.opts.Encoder
	})
}

func (s *server) Close() error {
	return s.ctrl.Close()
}

func (s *server) start() {
	go func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				s.logger.Error("Error accepting connection: %v", err)
				return
			}

			err = s.pool.SubmitOrCancel(s.ctrl.Closed(), s.newWorker(conn))
			if err != nil {
				conn.Close()
				continue
			}
		}
	}()
}

func (s *server) newWorker(conn net.Connection) func() {
	return func() {
		defer conn.Close()

		for {
			req, err := recvRequest(conn, s.opts.Encoder, s.opts.ReadTimeout)
			if err != nil {
				if err != io.EOF {
					s.logger.Error("Error receiving request [%v]: %v", err, conn.RemoteAddr())
				}
				return
			}

			if err = sendResponse(conn, s.handle(req), s.opts.Encoder, s.opts.SendTimeout); err != nil {
				s.logger.Error("Error sending response [%v]: %v", err, conn.RemoteAddr())
				return
			}
		}
	}
}

func (s *server) handle(req Request) (resp Response) {
	fn, ok := s.funcs[req.Func]
	if !ok {
		resp = NewErrorResponse(errors.Wrapf(ErrNoHandler, "No such handler [%v]", req.Func))
		return
	}

	resp = fn(req)
	return
}

func sendResponse(conn net.Connection, resp Response, enc enc.Encoder, timeout time.Duration) (err error) {
	var buf []byte
	if err = enc.EncodeBinary(resp, &buf); err != nil {
		return
	}
	if err = conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return
	}
	err = writePacketRaw(conn, newPacket(buf))
	return
}

func recvRequest(conn net.Connection, dec enc.Decoder, timeout time.Duration) (req Request, err error) {
	if err = conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return
	}

	p, err := readPacketRaw(conn)
	if err != nil {
		return
	}

	err = dec.DecodeBinary(p.Data, &req)
	return
}
