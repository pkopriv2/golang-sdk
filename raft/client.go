package raft

import (
	"github.com/pkopriv2/golang-sdk/lang/errs"
)

// This is the primary client implementation.  It uses the host's underlying
// transport to connect and send requests to remote hosts - which means that
// this is a concrete type
type Client struct {
	conn ClientSession
	opts Timeouts
}

func Dial(t Transport, addr string, o Timeouts) (ret *Client, err error) {
	conn, err := t.Dial(addr, o.DialTimeout)
	if err != nil {
		return
	}

	ret = &Client{conn, o}
	return
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Barrier() (ret ReadBarrierResponse, err error) {
	err = errs.Or(
		c.conn.Send(ReadBarrierRequest{}, c.opts.SendTimeout),
		c.conn.Read(&ret, c.opts.ReadTimeout))
	return
}

func (c *Client) Status() (ret StatusResponse, err error) {
	err = errs.Or(
		c.conn.Send(StatusRequest{}, c.opts.SendTimeout),
		c.conn.Read(&ret, c.opts.ReadTimeout))
	return
}

func (c *Client) RequestVote(req VoteRequest) (ret VoteResponse, err error) {
	err = errs.Or(
		c.conn.Send(req, c.opts.SendTimeout),
		c.conn.Read(&ret, c.opts.ReadTimeout))
	return
}

func (c *Client) UpdateRoster(req RosterUpdateRequest) error {
	var resp RosterUpdateResponse
	return errs.Or(
		c.conn.Send(req, c.opts.SendTimeout),
		c.conn.Read(&resp, c.opts.ReadTimeout))
}

func (c *Client) Replicate(req ReplicateRequest) (ret ReplicateResponse, err error) {
	err = errs.Or(
		c.conn.Send(req, c.opts.SendTimeout),
		c.conn.Read(&ret, c.opts.ReadTimeout))
	return
}

func (c *Client) Append(req AppendRequest) (ret AppendResponse, err error) {
	err = errs.Or(
		c.conn.Send(req, c.opts.SendTimeout),
		c.conn.Read(&ret, c.opts.ReadTimeout))
	return
}

func (c *Client) InstallSnapshotSegment(req InstallSnapshotRequest) (ret InstallSnapshotResponse, err error) {
	err = errs.Or(
		c.conn.Send(req, c.opts.SendTimeout),
		c.conn.Read(&ret, c.opts.ReadTimeout))
	return
}
