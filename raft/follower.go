package raft

import (
	"time"

	"github.com/pkopriv2/golang-sdk/lang/chans"
	"github.com/pkopriv2/golang-sdk/lang/context"
)

// The follower machine.  This
type follower struct {
	ctx     context.Context
	logger  context.Logger
	ctrl    context.Control
	term    term
	replica *replica
}

func becomeFollower(replica *replica) {
	ctx := replica.Ctx.Sub("Follower(%v)", replica.CurrentTerm())
	ctx.Logger().Info("Becoming follower")
	l := &follower{
		ctx:     ctx,
		logger:  ctx.Logger(),
		ctrl:    ctx.Control(),
		term:    replica.CurrentTerm(),
		replica: replica,
	}
	l.start()
}

func (c *follower) start() {
	// Proxy routine. (Out of band to allow replication requests through faster)
	go func() {
		defer c.ctrl.Close()
		for !c.ctrl.IsClosed() {
			select {
			case <-c.ctrl.Closed():
				return
			case req := <-c.replica.LocalAppends:
				c.handleLocalAppend(req)
			case req := <-c.replica.RemoteAppends:
				c.handleRemoteAppend(req)
			case req := <-c.replica.RosterUpdates:
				c.handleRosterUpdate(req)
			case req := <-c.replica.Barrier:
				c.handleReadBarrier(req)
			}
		}
	}()

	// Main routine
	go func() {
		defer c.ctrl.Close()
		c.ctrl.Defer(func(error) {
			c.logger.Info("Shutting down")
		})

		timer := time.NewTimer(c.replica.ElectionTimeout)
		defer timer.Stop()
		for !c.ctrl.IsClosed() {
			select {
			case <-c.ctrl.Closed():
				return
			case <-c.replica.ctrl.Closed():
				return
			case req := <-c.replica.Replications:
				c.handleReplication(req)
			case req := <-c.replica.VoteRequests:
				c.handleRequestVote(req)
			case req := <-c.replica.Snapshots:
				c.handleInstallSnapshotSegment(req)
			case <-timer.C:
				c.logger.Info("Waited too long for heartbeat: %v", c.replica.ElectionTimeout)
				becomeCandidate(c.replica)
				return
			}

			c.logger.Debug("Resetting election timeout: %v", c.replica.ElectionTimeout)
			timer.Reset(c.replica.ElectionTimeout)
		}
	}()
}

func (c *follower) handleLocalAppend(req *chans.Request) {
	req.Fail(ErrNotLeader)
}

func (c *follower) handleReadBarrier(req *chans.Request) {
	req.Fail(ErrNotLeader)
}

func (c *follower) handleRemoteAppend(req *chans.Request) {
	req.Fail(ErrNotLeader)
}

func (c *follower) handleRosterUpdate(req *chans.Request) {
	req.Fail(ErrNotLeader)
}

func (c *follower) handleInstallSnapshotSegment(req *chans.Request) {
	segment := req.Body().(installSnapshotRequest)
	if segment.Term < c.term.Num {
		req.Ack(installSnapshotResponse{Term: c.term.Num, Success: false})
		return
	}

	req.Ack(installSnapshotResponse{Term: segment.Term, Success: true})

	//if data == nil {
	//data, done = c.startSnapshotStream(segment)
	//offset = 0
	//}

	//if segment.BatchOffset != offset {
	//close(data)
	//data, done = c.startSnapshotStream(segment)
	//offset = 0
	//}

	//c.logger.Info("Installing snapshot: %v", segment)
	//if err := c.streamSnapshotSegment(data, segment); err != nil {
	//req.Fail(err)
	//return data, done, offset
	//}

	//c.logger.Error("Successfully installed segment: [%v/%v]", offset, segment.Size)

	//offset += int64(len(segment.Batch))
	//if offset < segment.Size {
	//req.Ack(installSnapshotResponse{Term: c.term.Num, Success: true})
	//return data, done, offset
	//}

	//select {
	//case r := <-done.Acked():
	//req.Ack(r)
	//case e := <-done.Failed():
	//req.Fail(e)
	//case <-c.ctrl.Closed():
	//req.Fail(ErrClosed)
	//}

	//return nil, nil, 0
}

func (c *follower) handleRequestVote(req *chans.Request) {
	vote := req.Body().(voteRequest)

	c.logger.Debug("Handling request vote [%v]", vote)

	// FIXME: Lots of duplicates here....condense down

	// handle: previous term vote.  (immediately decline.)
	if vote.Term < c.term.Num {
		req.Ack(voteResponse{Term: c.term.Num, Granted: false})
		return
	}

	// handle: current term vote.  (accept if no vote and if candidate log is as long as ours)
	maxIndex, maxTerm, err := c.replica.Log.LastIndexAndTerm()
	if err != nil {
		req.Ack(voteResponse{Term: c.term.Num, Granted: false})
		return
	}

	c.logger.Debug("Current log max: %v", maxIndex)
	if vote.Term == c.term.Num {
		if c.term.VotedFor == nil && vote.MaxLogIndex >= maxIndex && vote.MaxLogTerm >= maxTerm {
			c.logger.Debug("Voting for candidate [%v]", vote.Id)
			req.Ack(voteResponse{Term: vote.Term, Granted: true})
			c.replica.SetTerm(vote.Term, nil, &vote.Id) // correct?
			c.ctrl.Close()
			becomeFollower(c.replica)
			return
		}

		c.logger.Debug("Rejecting candidate vote [%v]", vote.Id)
		req.Ack(voteResponse{Term: vote.Term, Granted: false})
		c.ctrl.Close()
		becomeCandidate(c.replica)
		return
	}

	// handle: future term vote.  (move to new term.  only accept if candidate log is long enough)
	if vote.MaxLogIndex >= maxIndex && vote.MaxLogTerm >= maxTerm {
		c.logger.Debug("Voting for candidate [%v]", vote.Id)
		req.Ack(voteResponse{Term: vote.Term, Granted: true})
		c.replica.SetTerm(vote.Term, nil, &vote.Id)
		c.ctrl.Close()
		becomeFollower(c.replica)
		return
	}

	c.logger.Debug("Rejecting candidate vote [%v]", vote.Id)
	req.Ack(voteResponse{Term: vote.Term, Granted: false})
	c.replica.SetTerm(vote.Term, nil, nil)
	c.ctrl.Close()
	becomeCandidate(c.replica)
}

func (c *follower) handleReplication(req *chans.Request) {
	repl := req.Body().(replicateRequest)
	if repl.Term < c.term.Num {
		req.Ack(replicateResponse{Term: c.term.Num, Success: false})
		return
	}

	hint, _, err := c.replica.Log.LastIndexAndTerm()
	if err != nil {
		req.Fail(err)
		return
	}

	c.logger.Debug("Handling replication [%v]: %v", repl.PrevLogIndex, len(repl.Items))
	if repl.Term > c.term.Num || c.term.LeaderId == nil {
		c.logger.Info("New leader detected [%v]", repl.LeaderId.String()[:8])
		req.Ack(replicateResponse{Term: repl.Term, Success: false, Hint: hint})
		c.replica.SetTerm(repl.Term, &repl.LeaderId, &repl.LeaderId)
		c.ctrl.Close()
		becomeFollower(c.replica)
		return
	}

	// if this is a heartbeat, bail out
	if len(repl.Items) == 0 {
		req.Ack(replicateResponse{Term: repl.Term, Success: true})
		return
	}

	// consistency check
	ok, err := c.replica.Log.Assert(repl.PrevLogIndex, repl.PrevLogTerm)
	if err != nil {
		req.Fail(err)
		return
	}

	// consistency check failed.
	if !ok {
		c.logger.Debug("Consistency check failed. Responding with hint [%v]", hint)
		req.Ack(replicateResponse{Term: repl.Term, Success: false, Hint: hint})
		return
	}

	// insert items.
	if err := c.replica.Log.Insert(repl.Items); err != nil {
		c.logger.Error("Error inserting batch: %v", err)
		req.Fail(err)
		return
	}

	if _, err := c.replica.Log.Commit(repl.Commit); err != nil {
		c.logger.Info("Error committing offset: %v", err)
	}

	req.Ack(replicateResponse{Term: repl.Term, Success: true})
}
