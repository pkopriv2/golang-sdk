package rpc

import (
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkopriv2/golang-sdk/lang/net"
)

var (
	ErrNoHandler = fmt.Errorf("Rpc:NoHandler")
)

// Server implementation
func Serve(ctx context.Context, funcs Handlers, o ...Option) (ret Server, err error) {
	opts := buildOptions(o)

	raw, err := opts.Listener()
	if err != nil {
		return
	}

	ctx = ctx.Sub("RpcServer(%v)", raw.Address().String())
	ctx.Control().Defer(func(error) {
		raw.Close()
	})

	pool := NewWorkPool(ctx.Control(), opts.NumWorkers)
	ctx.Control().Defer(func(error) {
		pool.Close()
	})

	ctx.Control().Defer(func(cause error) {
		ctx.Logger().Info("Shutting down server: %+v", cause)
	})

	s := &server{
		context: ctx,
		ctrl:    ctx.Control(),
		logger:  ctx.Logger(),
		socket:  NewSocket(raw, opts.Encoder),
		funcs:   funcs,
		pool:    pool,
		opts:    opts}

	s.start()
	return s, nil
}

type server struct {
	context context.Context
	logger  context.Logger
	ctrl    context.Control
	pool    WorkPool
	funcs   Handlers
	socket  Socket
	opts    Options
}

func (s *server) Connect() (ret Client, err error) {
	if s.ctrl.IsClosed() {
		err = errs.ClosedError
		return
	}

	dialer := func(t time.Duration) (net.Connection, error) {
		return s.socket.(*socket).raw.Connect(t)
	}

	conn, err := Dial(dialer,
		WithDialTimeout(s.opts.DialTimeout),
		WithEncoder(s.opts.Encoder))
	if err != nil {
		return
	}

	ret = NewClient(conn,
		WithSendTimeout(s.opts.SendTimeout),
		WithReadTimeout(s.opts.ReadTimeout))
	return
}

func (s *server) Close() error {
	return s.ctrl.Close()
}

func (s *server) start() {
	go func() {
		for {
			session, err := s.socket.Accept()
			if err != nil {
				s.logger.Error("Error accepting connection: %v", err)
				return
			}

			if err = s.pool.SubmitOrCancel(s.ctrl.Closed(), s.newWorker(session)); err != nil {
				session.Close()
				continue
			}
		}
	}()
}

func (s *server) newWorker(session ServerSession) func() {
	return func() {
		defer session.Close()

		for {
			req, err := session.Read(s.opts.ReadTimeout)
			if err != nil {
				if err != io.EOF {
					s.logger.Error("Error receiving request [%v]: %v", err, session.RemoteAddr())
				}
				return
			}

			if err = session.Send(s.handle(req), s.opts.SendTimeout); err != nil {
				s.logger.Error("Error sending response [%v]: %v", err, session.RemoteAddr())
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
