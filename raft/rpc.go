package raft

import (
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkopriv2/golang-sdk/lang/net"
	"github.com/pkopriv2/golang-sdk/rpc"
)

// This file contains the RPC Transport definitions.
const (
	funcReadBarrier     = "raft.readBarrier"
	funcStatus          = "raft.status"
	funcRequestVote     = "raft.requestVote"
	funcUpdateRoster    = "raft.roster"
	funcReplicate       = "raft.replicate"
	funcAppend          = "raft.append"
	funcInstallSnapshot = "raft.installSnapshot"
)

type RpcTransport struct {
	raw net.Network
	enc enc.EncoderDecoder
}

func NewRpcTransport(net net.Network, enc enc.EncoderDecoder) Transport {
	return &RpcTransport{net, enc}
}

func (r *RpcTransport) Dial(addr string, timeout time.Duration) (ret ClientSession, err error) {
	raw, err := rpc.Dial(
		rpc.NewDialer(r.raw, addr),
		rpc.WithDialTimeout(timeout),
		rpc.WithEncoder(r.enc))
	if err != nil {
		return
	}

	ret = &RpcClientSession{raw, r.enc}
	return
}

func (r *RpcTransport) Listen(addr string) (ret Socket, err error) {
	raw, err := r.raw.Listen(addr)
	if err != nil {
		return
	}

	ret = &RpcSocket{rpc.NewSocket(raw, r.enc), r.enc}
	return
}

type RpcSocket struct {
	raw rpc.Socket
	enc enc.EncoderDecoder
}

func (r *RpcSocket) Close() error {
	return r.raw.Close()
}

func (r *RpcSocket) Addr() string {
	return r.raw.Addr()
}

func (r *RpcSocket) Accept() (ret ServerSession, err error) {
	raw, err := r.raw.Accept()
	if err != nil {
		return
	}

	ret = &RpcServerSession{raw, r.enc}
	return
}

type RpcServerSession struct {
	raw rpc.ServerSession
	enc enc.EncoderDecoder
}

func (r *RpcServerSession) Close() error {
	return r.raw.Close()
}

func (r *RpcServerSession) LocalAddr() string {
	return r.raw.LocalAddr()
}

func (r *RpcServerSession) RemoteAddr() string {
	return r.raw.RemoteAddr()
}

func (r *RpcServerSession) ReadRequest(timeout time.Duration) (ret interface{}, err error) {
	req, err := r.raw.Read(timeout)
	if err != nil {
		return
	}

	var ptr interface{}
	switch req.Func {
	default:
		err = errors.Wrapf(ErrInvalid, "Invalid function handler [%v]", req.Func)
		return
	case funcReadBarrier:
		ptr = &ReadBarrierRequest{}
	case funcStatus:
		ptr = &StatusRequest{}
	case funcRequestVote:
		ptr = &VoteRequest{}
	case funcUpdateRoster:
		ptr = &RosterUpdateRequest{}
	case funcReplicate:
		ptr = &ReplicateRequest{}
	case funcAppend:
		ptr = &AppendRequest{}
	case funcInstallSnapshot:
		ptr = &InstallSnapshotRequest{}
	}
	if err = req.Decode(r.enc, ptr); err != nil {
		return
	}

	ret = reflect.ValueOf(ptr).Elem().Interface()
	return
}

func (r *RpcServerSession) SendResponse(val interface{}, timeout time.Duration) error {
	if val == nil {
		return r.raw.Send(rpc.EmptyResponse, timeout)
	}
	if err, ok := val.(error); ok {
		return r.raw.Send(rpc.NewErrorResponse(err), timeout)
	} else {
		return r.raw.Send(rpc.NewStructResponse(r.enc, val), timeout)
	}
}

type RpcClientSession struct {
	raw rpc.ClientSession
	enc enc.EncoderDecoder
}

func (c *RpcClientSession) Close() error {
	return c.raw.Close()
}

func (r *RpcClientSession) ReadResponse(ptr interface{}, timeout time.Duration) (err error) {
	resp, err := r.raw.Read(timeout)
	if err != nil || !resp.Ok {
		err = errs.Or(err, resp.Error())
		return
	}

	err = resp.Decode(r.enc, ptr)
	return
}

func (r *RpcClientSession) SendRequest(val interface{}, timeout time.Duration) (err error) {
	var fn string
	switch val.(type) {
	default:
		err = errors.Wrapf(ErrInvalid, "Invalid request type [%v]", reflect.TypeOf(val))
		return
	case ReadBarrierRequest:
		fn = funcReadBarrier
	case StatusRequest:
		fn = funcStatus
	case VoteRequest:
		fn = funcRequestVote
	case RosterUpdateRequest:
		fn = funcUpdateRoster
	case ReplicateRequest:
		fn = funcReplicate
	case AppendRequest:
		fn = funcAppend
	case InstallSnapshotRequest:
		fn = funcInstallSnapshot
	}

	req, err := rpc.NewStructRequest(fn, r.enc, val)
	if err != nil {
		return
	}

	return r.raw.Send(req, timeout)
}
