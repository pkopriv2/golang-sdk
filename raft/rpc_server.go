package raft

import (
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/net"
	"github.com/pkopriv2/golang-sdk/rpc"
)

type rpcServer struct {
	ctx    context.Context
	logger context.Logger
	self   *replica
	enc    enc.EncoderDecoder
}

// Returns a new service handler for the ractlica
func newServer(ctx context.Context, self *replica, listener net.Listener) (rpc.Server, error) {
	server := &rpcServer{
		ctx:    ctx,
		logger: ctx.Logger(),
		self:   self,
		enc:    self.Options.Encoder}

	return rpc.Serve(ctx,
		rpc.BuildHandlers(
			rpc.WithHandler(funcStatus, server.Status),
			rpc.WithHandler(funcReadBarrier, server.ReadBarrier),
			rpc.WithHandler(funcRequestVote, server.RequestVote),
			rpc.WithHandler(funcUpdateRoster, server.UpdateRoster),
			rpc.WithHandler(funcReplicate, server.Replicate),
			rpc.WithHandler(funcAppend, server.Append),
			rpc.WithHandler(funcInstallSnapshot, server.InstallSnapshot),
		),
		rpc.WithListener(listener),
		rpc.WithNumWorkers(self.Options.Workers),
		rpc.WithEncoder(self.Options.Encoder))
}

func (s *rpcServer) Status(req rpc.Request) rpc.Response {
	resp := statusResponse{
		Self:   s.self.Self,
		Term:   s.self.CurrentTerm(),
		Config: Config{s.self.Cluster()}}
	return rpc.NewStructResponse(s.enc, resp)
}

func (s *rpcServer) ReadBarrier(req rpc.Request) rpc.Response {
	val, err := s.self.ReadBarrier()
	if err != nil {
		return rpc.NewErrorResponse(err)
	}

	return rpc.NewStructResponse(s.enc,
		readBarrierResponse{
			Barrier: val,
		})
}

func (s *rpcServer) UpdateRoster(raw rpc.Request) rpc.Response {
	var req rosterUpdateRequest
	if err := raw.Decode(s.enc, &req); err != nil {
		return rpc.NewErrorResponse(err)
	}

	return rpc.NewErrorResponse(s.self.UpdateRoster(req))
}

func (s *rpcServer) InstallSnapshot(raw rpc.Request) rpc.Response {
	var req installSnapshotRequest
	if err := raw.Decode(s.enc, &req); err != nil {
		return rpc.NewErrorResponse(err)
	}

	resp, err := s.self.InstallSnapshot(req)
	if err != nil {
		return rpc.NewErrorResponse(err)
	}

	return rpc.NewStructResponse(s.enc, resp)
}

func (s *rpcServer) Replicate(raw rpc.Request) rpc.Response {
	var req replicateRequest
	if err := raw.Decode(s.enc, &req); err != nil {
		return rpc.NewErrorResponse(err)
	}

	resp, err := s.self.Replicate(req)
	if err != nil {
		return rpc.NewErrorResponse(err)
	}

	return rpc.NewStructResponse(s.enc, resp)
}

func (s *rpcServer) RequestVote(raw rpc.Request) rpc.Response {
	var req voteRequest
	if err := raw.Decode(s.enc, &req); err != nil {
		return rpc.NewErrorResponse(err)
	}

	resp, err := s.self.RequestVote(req)
	if err != nil {
		return rpc.NewErrorResponse(err)
	}

	return rpc.NewStructResponse(s.enc, resp)
}

func (s *rpcServer) Append(raw rpc.Request) rpc.Response {
	var req appendEventRequest
	if err := raw.Decode(s.enc, &req); err != nil {
		return rpc.NewErrorResponse(err)
	}

	item, err := s.self.RemoteAppend(req)
	if err != nil {
		return rpc.NewErrorResponse(err)
	}

	return rpc.NewStructResponse(s.enc, appendEventResponse{
		Index: item.Index,
		Term:  item.Term,
	})
}
