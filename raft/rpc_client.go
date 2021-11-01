package raft

import (
	"time"

	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkopriv2/golang-sdk/lang/net"
	"github.com/pkopriv2/golang-sdk/rpc"
)

type rpcClient struct {
	raw rpc.Client
	enc enc.EncoderDecoder
}

func connect(ctx context.Context, network net.Network, timeout time.Duration, addr string) (ret *rpcClient, err error) {
	client, err := rpc.Dial(rpc.NewDialer(network, addr), rpc.WithDialTimeout(timeout))
	if err != nil {
		return
	}

	ret = &rpcClient{
		raw: client,
		enc: enc.Gob}
	return
}

func (c *rpcClient) Close() error {
	return c.raw.Close()
}

func (c *rpcClient) Barrier() (int, error) {
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

func (c *rpcClient) InstallSnapshot(cancel <-chan struct{}, snapshot installSnapshotRequest) (ret installSnapshotResponse, err error) {
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

//type rpcClientPool struct {
//ctx context.Context
//raw context.ObjectPool
//}

//func newRpcClientPool(ctx context.Context, network net.Network, peer Peer, size int) *rpcClientPool {
//return &rpcClientPool{ctx, context.NewObjectPool(ctx.Control(), size, newRpcClientConstructor(ctx, network, peer))}
//}

//func (c *rpcClientPool) Close() error {
//return c.raw.Close()
//}

//func (c *rpcClientPool) Max() int {
//return c.raw.Max()
//}

//func (c *rpcClientPool) TakeTimeout(dur time.Duration) *rpcClient {
//raw := c.raw.TakeTimeout(dur)
//if raw == nil {
//return nil
//}

//return raw.(*rpcClient)
//}

//func (c *rpcClientPool) Return(cl *rpcClient) {
//c.raw.Return(cl)
//}

//func (c *rpcClientPool) Fail(cl *rpcClient) {
//c.raw.Fail(cl)
//}

//func newRpcClientConstructor(ctx context.Context, network net.Network, peer Peer) func() (io.Closer, error) {
//return func() (io.Closer, error) {
//if cl, err := peer.Client(ctx, network, 30*time.Second); cl != nil && err == nil {
//return cl, err
//}

//return nil, nil
//}
//}
