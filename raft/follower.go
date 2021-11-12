package raft

import (
	"time"

	"github.com/pkopriv2/golang-sdk/lang/chans"
	"github.com/pkopriv2/golang-sdk/lang/context"
)

// This implements the follower state machine.  Because this is steady-state
// machine, it must implement all of the requests types.  In many instances,
// requests can only be processed by a leader.  In situations where it
// receives leader-only requests, it responds with ErrNotLeader
//
// The follower reads requests from the replica instance and responds to them.
type follower struct {
	ctx     context.Context
	logger  context.Logger
	ctrl    context.Control
	term    Term
	replica *replica
}

func becomeFollower(replica *replica) {
	ctx := replica.Ctx.Sub("Follower(%v)", replica.CurrentTerm())
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
			case req := <-c.replica.AppendRequests:
				c.handleAppend(req)
			case req := <-c.replica.RosterUpdateRequests:
				c.handleRosterUpdate(req)
			case req := <-c.replica.BarrierRequests:
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
			case req := <-c.replica.ReplicationRequests:
				c.handleReplication(req)
			case req := <-c.replica.VoteRequests:
				c.handleRequestVote(req)
			case req := <-c.replica.SnapshotRequests:
				c.handleInstallSnapshotSegment(req)
			case <-timer.C:
				c.logger.Info("Waited too long for heartbeat: %v", c.replica.ElectionTimeout)
				c.ctrl.Close()
				becomeCandidate(c.replica)
				return
			}

			c.logger.Debug("Resetting election timeout: %v", c.replica.ElectionTimeout)
			timer.Reset(c.replica.ElectionTimeout)
		}
	}()
}

func (c *follower) handleAppend(req *chans.Request) {
	req.Fail(ErrNotLeader)
}

func (c *follower) handleReadBarrier(req *chans.Request) {
	req.Fail(ErrNotLeader)
}

func (c *follower) handleRosterUpdate(req *chans.Request) {
	req.Fail(ErrNotLeader)
}

func (c *follower) handleInstallSnapshotSegment(req *chans.Request) {
	segment := req.Body().(InstallSnapshotRequest)
	if segment.Term < c.term.Epoch {
		req.Ack(InstallSnapshotResponse{Term: c.term.Epoch, Success: false})
		return
	}

	c.logger.Debug("Installing snapshot segment [offset=%v,num=%v]", segment.BatchOffset, len(segment.Batch))

	store := c.replica.Log.Store()
	if err := store.InstallSnapshotSegment(segment.Id, segment.BatchOffset, segment.Batch); err != nil {
		req.Fail(err)
		return
	}

	if segment.BatchOffset+int64(len(segment.Batch)) < segment.Size {
		req.Ack(InstallSnapshotResponse{Term: c.term.Epoch, Success: true})
		return
	}

	snapshot, err := store.InstallSnapshot(segment.Id, segment.MaxIndex, segment.MaxTerm, segment.Size, segment.Config)
	if err != nil {
		c.logger.Error("Error installing snapshot [%v]: %v", segment.Id, err)
		req.Fail(err)
		return
	}

	c.logger.Info("Successfully installed snapshot [id=%v,size=%v]", snapshot.Id().String()[:8], snapshot.Size())
	req.Ack(InstallSnapshotResponse{Term: c.term.Epoch, Success: true})
}

func (c *follower) handleRequestVote(req *chans.Request) {
	vote := req.Body().(VoteRequest)

	c.logger.Debug("Handling request vote [term=%v,peer=%v]", vote.Term, vote.Id.String()[:8])

	// previous term vote.  (immediately decline.)
	if vote.Term < c.term.Epoch {
		req.Ack(VoteResponse{Term: c.term.Epoch, Granted: false})
		return
	}

	// current term vote.  (accept if no vote and if candidate log is as long as ours)
	maxIndex, maxTerm, err := c.replica.Log.LastIndexAndTerm()
	if err != nil {
		req.Fail(err)
		return
	}

	// If the replica isn't recognized, deny the vote. Immediately become candidate
	_, ok := c.replica.FindPeer(vote.Id)
	if !ok {
		req.Ack(VoteResponse{Term: vote.Term, Granted: false})
		c.replica.SetTerm(vote.Term, nil, nil)
		c.ctrl.Close()
		becomeCandidate(c.replica)
		return
	}

	if vote.Term == c.term.Epoch {
		if c.term.VotedFor == nil && vote.MaxLogIndex >= maxIndex && vote.MaxLogTerm >= maxTerm {
			c.logger.Debug("Voting for candidate [%v]", vote.Id)
			req.Ack(VoteResponse{Term: vote.Term, Granted: true})
			c.replica.SetTerm(vote.Term, nil, &vote.Id) // correct?
			c.ctrl.Close()
			becomeFollower(c.replica)
			return
		}

		c.logger.Debug("Rejecting candidate vote [%v]", vote.Id)
		req.Ack(VoteResponse{Term: vote.Term, Granted: false})
		c.ctrl.Close()
		becomeCandidate(c.replica)
		return
	}

	// handle: future term vote.  (move to new term.  only accept if candidate log is long enough)
	if vote.MaxLogIndex >= maxIndex && vote.MaxLogTerm >= maxTerm {
		c.logger.Debug("Voting for candidate [%v]", vote.Id)
		req.Ack(VoteResponse{Term: vote.Term, Granted: true})
		c.replica.SetTerm(vote.Term, nil, &vote.Id)
		c.ctrl.Close()
		becomeFollower(c.replica)
		return
	}

	c.logger.Debug("Rejecting candidate vote [%v]", vote.Id)
	req.Ack(VoteResponse{Term: vote.Term, Granted: false})
	c.replica.SetTerm(vote.Term, nil, nil)
	c.ctrl.Close()
	becomeCandidate(c.replica)
}

func (c *follower) handleReplication(req *chans.Request) {
	repl := req.Body().(ReplicateRequest)
	if repl.Term < c.term.Epoch {
		req.Ack(ReplicateResponse{Term: c.term.Epoch, Success: false})
		return
	}

	c.logger.Debug("Handling replication [prevIndex=%v, prevTerm=%v]", repl.PrevLogIndex, repl.PrevLogTerm)

	if repl.Term > c.term.Epoch || c.term.LeaderId == nil {
		c.logger.Info("New leader detected [%v]", repl.LeaderId.String()[:8])

		hint, _, err := c.replica.Log.LastIndexAndTerm()
		if err != nil {
			req.Fail(err)
			return
		}

		req.Ack(ReplicateResponse{Term: repl.Term, Success: false, Hint: hint})
		c.replica.SetTerm(repl.Term, &repl.LeaderId, &repl.LeaderId)
		c.ctrl.Close()
		becomeFollower(c.replica)
		return
	}

	c.logger.Debug("Committing [%v]", repl.Commit)
	c.replica.Log.Commit(repl.Commit)

	// if this is a heartbeat, bail out
	if len(repl.Items) == 0 {
		req.Ack(ReplicateResponse{Term: repl.Term, Success: true})
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
		hint, _, err := c.replica.Log.LastIndexAndTerm()
		if err != nil {
			req.Fail(err)
			return
		}

		c.logger.Debug("Consistency check failed. Responding with hint [%v]", hint)
		req.Ack(ReplicateResponse{Term: repl.Term, Success: false, Hint: hint})
		return
	}

	// insert items.
	c.logger.Debug("Inserting batch [offset=%v,num=%v]", repl.Items[0].Index, len(repl.Items))
	if err := c.replica.Log.Insert(repl.Items); err != nil {
		req.Fail(err)
		return
	}

	if repl.Commit <= repl.Items[len(repl.Items)-1].Index {
		c.replica.Log.Commit(repl.Commit)
	}

	req.Ack(ReplicateResponse{Term: repl.Term, Success: true})
}
