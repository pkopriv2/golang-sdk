package raft

import (
	"reflect"
	"time"

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

func (r *RpcTransport) Dial(addr string, opts Timeouts) (ret Client, err error) {
	raw, err := rpc.Dial(rpc.NewDialer(r.raw, addr),
		rpc.WithReadTimeout(opts.ReadTimeout),
		rpc.WithDialTimeout(opts.DialTimeout),
		rpc.WithSendTimeout(opts.SendTimeout),
		rpc.WithEncoder(r.enc))
	if err != nil {
		return
	}

	ret = &RpcClient{raw, r.enc}
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

func (r *RpcSocket) Accept() (ret Session, err error) {
	raw, err := r.raw.Accept()
	if err != nil {
		return
	}

	ret = &RpcSession{raw, r.enc}
	return
}

type RpcSession struct {
	raw rpc.Session
	enc enc.EncoderDecoder
}

func (r *RpcSession) Close() error {
	return r.raw.Close()
}

func (r *RpcSession) LocalAddr() string {
	return r.raw.LocalAddr()
}

func (r *RpcSession) RemoteAddr() string {
	return r.raw.RemoteAddr()
}

func (r *RpcSession) Read(timeout time.Duration) (ret interface{}, err error) {
	req, err := r.raw.Read(timeout)
	if err != nil {
		return
	}

	var ptr interface{}
	switch req.Func {
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
		ptr = &AppendEventRequest{}
	case funcInstallSnapshot:
		ptr = &InstallSnapshotRequest{}
	}
	if err = req.Decode(r.enc, ptr); err != nil {
		return
	}

	ret = reflect.ValueOf(ptr).Elem().Interface()
	return
}

func (r *RpcSession) Send(val interface{}, timeout time.Duration) error {
	if val == nil {
		return r.raw.Send(rpc.EmptyResponse, timeout)
	}
	if err, ok := val.(error); ok {
		return r.raw.Send(rpc.NewErrorResponse(err), timeout)
	} else {
		return r.raw.Send(rpc.NewStructResponse(r.enc, val), timeout)
	}
}

type RpcClient struct {
	raw rpc.Client
	enc enc.EncoderDecoder
}

func (c *RpcClient) Close() error {
	return c.raw.Close()
}

func (c *RpcClient) Barrier() (ret ReadBarrierResponse, err error) {
	req, err := rpc.NewStructRequest(funcReadBarrier, c.enc, ReadBarrierRequest{})
	if err != nil {
		return
	}

	resp, err := c.raw.Send(req)
	if err != nil || !resp.Ok {
		err = errs.Or(err, resp.Error())
		return
	}

	err = resp.Decode(c.enc, &ret)
	return
}

func (c *RpcClient) Status() (ret StatusResponse, err error) {
	req, err := rpc.NewStructRequest(funcStatus, c.enc, StatusRequest{})
	if err != nil {
		return
	}

	resp, err := c.raw.Send(req)
	if err != nil || !resp.Ok {
		err = errs.Or(err, resp.Error())
		return
	}

	err = resp.Decode(c.enc, &ret)
	return
}

func (c *RpcClient) RequestVote(vote VoteRequest) (ret VoteResponse, err error) {
	req, err := rpc.NewStructRequest(funcRequestVote, c.enc, vote)
	if err != nil {
		return
	}

	resp, err := c.raw.Send(req)
	if err != nil || !resp.Ok {
		err = errs.Or(err, resp.Error())
		return
	}

	err = resp.Decode(c.enc, &ret)
	return
}

func (c *RpcClient) UpdateRoster(updateRoster RosterUpdateRequest) error {
	req, err := rpc.NewStructRequest(funcUpdateRoster, c.enc, updateRoster)
	if err != nil {
		return err
	}

	resp, err := c.raw.Send(req)
	if err != nil || !resp.Ok {
		return errs.Or(err, resp.Error())
	}

	return nil
}

func (c *RpcClient) Replicate(r ReplicateRequest) (ret ReplicateResponse, err error) {
	req, err := rpc.NewStructRequest(funcReplicate, c.enc, r)
	if err != nil {
		return
	}

	resp, err := c.raw.Send(req)
	if err != nil || !resp.Ok {
		err = errs.Or(err, resp.Error())
		return
	}

	err = resp.Decode(c.enc, &ret)
	return
}

func (c *RpcClient) Append(r AppendEventRequest) (ret AppendEventResponse, err error) {
	req, err := rpc.NewStructRequest(funcAppend, c.enc, r)
	if err != nil {
		return
	}

	resp, err := c.raw.Send(req)
	if err != nil || !resp.Ok {
		err = errs.Or(err, resp.Error())
		return
	}

	err = resp.Decode(c.enc, &ret)
	return
}

func (c *RpcClient) InstallSnapshotSegment(snapshot InstallSnapshotRequest) (ret InstallSnapshotResponse, err error) {
	req, err := rpc.NewStructRequest(funcInstallSnapshot, c.enc, snapshot)
	if err != nil {
		return
	}

	resp, err := c.raw.Send(req)
	if err != nil || !resp.Ok {
		err = errs.Or(err, resp.Error())
		return
	}

	err = resp.Decode(c.enc, &ret)
	return
}
