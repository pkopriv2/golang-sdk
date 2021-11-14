package raft

// This is the high-level client implementation that is used internally.
// It uses the host's underlying transport to connect and send requests
// to remote hosts.
type Client struct {
	conn ClientSession
	opts Timeouts
}

// Dial the address on the given transport.  Returns a high-level client
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

func (c *Client) Send(req interface{}, respPtr interface{}) error {
	if err := c.conn.SendRequest(req, c.opts.SendTimeout); err != nil {
		return err
	}
	return c.conn.ReadResponse(respPtr, c.opts.ReadTimeout)
}

func (c *Client) Barrier() (ret ReadBarrierResponse, err error) {
	err = c.Send(ReadBarrierRequest{}, &ret)
	return
}

func (c *Client) Status() (ret StatusResponse, err error) {
	err = c.Send(StatusRequest{}, &ret)
	return
}

func (c *Client) RequestVote(req VoteRequest) (ret VoteResponse, err error) {
	err = c.Send(req, &ret)
	return
}

func (c *Client) UpdateRoster(req RosterUpdateRequest) error {
	var resp RosterUpdateResponse
	return c.Send(req, &resp)
}

func (c *Client) Replicate(req ReplicateRequest) (ret ReplicateResponse, err error) {
	err = c.Send(req, &ret)
	return
}

func (c *Client) Append(req AppendRequest) (ret AppendResponse, err error) {
	err = c.Send(req, &ret)
	return
}

func (c *Client) InstallSnapshotSegment(req InstallSnapshotRequest) (ret InstallSnapshotResponse, err error) {
	err = c.Send(req, &ret)
	return
}
