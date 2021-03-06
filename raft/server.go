package raft

import (
	"io"
	"reflect"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/pool"
)

// This is the core server instance that receives reqeusts from clients over
// the host's transport. The server typically routes requests to the replica
// and backing state machine.
type server struct {
	ctx    context.Context
	ctrl   context.Control
	logger context.Logger
	self   *replica
	opts   Options
	pool   pool.WorkPool
}

func newServer(ctx context.Context, self *replica, socket Socket, opts Options) (ret *server) {
	ctx = ctx.Sub("Server(%v)", self.Self)
	ret = &server{
		ctx:    ctx,
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
		self:   self,
		opts:   opts,
		pool:   pool.NewWorkPool(ctx.Control(), opts.MaxWorkers),
	}
	ret.start(socket)
	return
}

func (s *server) Close() error {
	return s.ctrl.Close()
}

func (s *server) start(socket Socket) {
	go func() {
		defer socket.Close()
		for {
			session, err := socket.Accept()
			if err != nil {
				s.logger.Debug("Error accepting connection: %v", err)
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
			req, err := session.ReadRequest(s.opts.ReadTimeout)
			if err != nil {
				if err != io.EOF {
					s.logger.Debug("Error receiving request [%v]: %v", err, session.RemoteAddr())
				}
				return
			}

			var resp interface{}
			switch r := req.(type) {
			default:
				err = errors.Wrapf(ErrInvalid, "Invalid request type [%v]", reflect.ValueOf(req))
			case StatusRequest:
				resp = StatusResponse{
					Self:   s.self.Self,
					Term:   s.self.CurrentTerm(),
					Config: Config{s.self.Cluster()},
				}
			case ReadBarrierRequest:
				resp, err = s.self.ReadBarrier()
			case ReplicateRequest:
				resp, err = s.self.Replicate(r)
			case VoteRequest:
				resp, err = s.self.RequestVote(r)
			case AppendRequest:
				resp, err = s.self.Append(r)
			case InstallSnapshotRequest:
				resp, err = s.self.InstallSnapshot(r)
			case RosterUpdateRequest:
				resp, err = RosterUpdateResponse{}, s.self.UpdateRoster(r)
			}
			if err != nil {
				if e := session.SendResponse(err, s.opts.SendTimeout); e != nil {
					s.logger.Error("Error sending response [%v]: %v", e, session.RemoteAddr())
					return
				}
			} else {
				if e := session.SendResponse(resp, s.opts.SendTimeout); e != nil {
					s.logger.Error("Error sending response [%v]: %v", e, session.RemoteAddr())
					return
				}
			}
		}
	}
}
