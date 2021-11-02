package raft

import (
	"time"

	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkopriv2/golang-sdk/lang/net"
	"github.com/pkopriv2/golang-sdk/rpc"
)

type rpcClient struct {
	raw rpc.Client
	enc enc.EncoderDecoder
}

func dialRpcClient(net net.Network, timeout time.Duration, addr string, enc enc.EncoderDecoder) (ret *rpcClient, err error) {
	client, err := rpc.Dial(rpc.NewDialer(net, addr), rpc.WithDialTimeout(timeout))
	if err != nil {
		return
	}

	ret = &rpcClient{
		raw: client,
		enc: enc}
	return
}

func (c *rpcClient) Close() error {
	return c.raw.Close()
}

func (c *rpcClient) Barrier() (int64, error) {
	resp, err := c.raw.Send(
		rpc.Request{
			Func: funcReadBarrier,
		})
	if err != nil || !resp.Ok {
		return -1, errs.Or(err, resp.Error)
	}

	var r readBarrierResponse
	if err := resp.Decode(c.enc, &r); err != nil {
		return -1, err
	}

	return r.Barrier, nil
}

func (c *rpcClient) Status() (ret statusResponse, err error) {
	resp, err := c.raw.Send(
		rpc.Request{
			Func: funcStatus,
		})
	if err != nil || !resp.Ok {
		err = errs.Or(err, resp.Error)
		return
	}

	err = resp.Decode(c.enc, &ret)
	return
}

func (c *rpcClient) RequestVote(vote voteRequest) (ret voteResponse, err error) {
	var bytes []byte
	if err = c.enc.EncodeBinary(vote, &bytes); err != nil {
		return
	}

	resp, err := c.raw.Send(
		rpc.Request{
			Func: funcRequestVote,
			Body: bytes,
		})
	if err != nil || !resp.Ok {
		err = errs.Or(err, resp.Error)
		return
	}

	err = resp.Decode(c.enc, &ret)
	return
}

func (c *rpcClient) UpdateRoster(peer Peer, join bool) error {
	var bytes []byte
	if err := c.enc.EncodeBinary(rosterUpdateRequest{peer, join}, &bytes); err != nil {
		return err
	}

	resp, err := c.raw.Send(
		rpc.Request{
			Func: funcUpdateRoster,
			Body: bytes,
		})
	return errs.Or(err, resp.Error)
}

func (c *rpcClient) Replicate(r replicateRequest) (ret replicateResponse, err error) {
	var bytes []byte
	if err = c.enc.EncodeBinary(r, &bytes); err != nil {
		return
	}

	resp, err := c.raw.Send(
		rpc.Request{
			Func: funcReplicate,
			Body: bytes,
		})
	if err != nil || !resp.Ok {
		err = errs.Or(err, resp.Error)
		return
	}

	err = resp.Decode(c.enc, &ret)
	return
}

func (c *rpcClient) Append(r appendEventRequest) (ret appendEventResponse, err error) {
	var bytes []byte
	if err = c.enc.EncodeBinary(r, &bytes); err != nil {
		return
	}

	resp, err := c.raw.Send(
		rpc.Request{
			Func: funcAppend,
			Body: bytes,
		})
	if err != nil || !resp.Ok {
		err = errs.Or(err, resp.Error)
		return
	}

	err = resp.Decode(c.enc, &ret)
	return
}

func (c *rpcClient) InstallSnapshotSegment(snapshot installSnapshotRequest) (ret installSnapshotResponse, err error) {
	var bytes []byte
	if err = c.enc.EncodeBinary(snapshot, &bytes); err != nil {
		return
	}

	resp, err := c.raw.Send(
		rpc.Request{
			Func: funcInstallSnapshot,
			Body: bytes,
		})
	if err != nil || !resp.Ok {
		err = errs.Or(err, resp.Error)
		return
	}

	err = resp.Decode(c.enc, &ret)
	return
}
