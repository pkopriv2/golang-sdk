package raft

import (
	"sync"
	"time"

	"github.com/pkopriv2/golang-sdk/lang/chans"
	"github.com/pkopriv2/golang-sdk/lang/context"
)

// The candidate implements the candidate state machine.  This currently only
// implements a small subset of the request types since it is expected to
// be relatively transient.
//
// The candidate reads requests from the replica instance and responds to them.
type candidate struct {
	ctx     context.Context
	ctrl    context.Control
	logger  context.Logger
	term    Term
	replica *replica
}

func becomeCandidate(replica *replica) {
	// increment term and vote for self.
	replica.SetTerm(replica.CurrentTerm().Epoch+1, nil, &replica.Self.Id)

	// We need a new root context so we don't leak defers on the replica's context.
	// Basically, anything that is part of the leader's lifecycle needs to be its
	// own isolated object hierarchy.
	ctx := replica.NewRootContext().Sub("%v: Candidate(%v)", replica.Self, replica.CurrentTerm())
	ctx.Logger().Info("Becoming candidate")

	l := &candidate{
		ctx:     ctx,
		logger:  ctx.Logger(),
		ctrl:    ctx.Control(),
		term:    replica.CurrentTerm(),
		replica: replica,
	}

	l.start()
}

func (c *candidate) start() {

	maxIndex, maxTerm, err := c.replica.Log.LastIndexAndTerm()
	if err != nil {
		c.ctrl.Fail(err)
		becomeFollower(c.replica)
		return
	}

	c.logger.Debug("Sending ballots: (term=%v,maxIndex=%v,maxTerm=%v)", c.term.Epoch, maxIndex, maxTerm)

	wg := &sync.WaitGroup{}
	ballots := c.replica.Broadcast(func(cl *Client) (interface{}, error) {
		wg.Add(1)
		defer wg.Done()
		return cl.RequestVote(
			VoteRequest{
				Id:          c.replica.Self.Id,
				Term:        c.term.Epoch,
				MaxLogIndex: maxIndex,
				MaxLogTerm:  maxTerm,
			})
	})

	// We want to block any transition until the ballots have all been received.
	c.ctrl.Defer(func(error) {
		wg.Wait()
	})

	// Currently this is essentially a single-threaded implementation.
	go func() {
		defer c.ctrl.Close()
		defer func() {
			c.logger.Info("Shutting down")
		}()

		// set the election timer.
		c.logger.Info("Setting election timer [%v]", c.replica.ElectionTimeout)
		timer := time.NewTimer(c.replica.ElectionTimeout)
		defer timer.Stop()

		for numVotes := 1; !c.ctrl.IsClosed(); {
			needed := c.replica.Majority()
			c.logger.Info("Received [%v/%v] votes", numVotes, needed)

			if numVotes >= needed {
				c.logger.Info("Acquired majority [%v] votes.", needed)
				c.replica.SetTerm(c.replica.term.Epoch, &c.replica.Self.Id, &c.replica.Self.Id)
				becomeLeader(c.replica)
				c.ctrl.Close() // this is the one case where it's okay to close after transitioning...need to send heartbeat immediately
				return
			}

			select {
			case <-c.ctrl.Closed():
				return
			case <-c.replica.ctrl.Closed():
				return
			case req := <-c.replica.AppendRequests:
				c.handleAppend(req)
			case req := <-c.replica.BarrierRequests:
				c.handleReadBarrier(req)
			case req := <-c.replica.RosterUpdateRequests:
				c.handleRosterUpdate(req)
			case req := <-c.replica.ReplicationRequests:
				c.handleReplication(req)
			case req := <-c.replica.VoteRequests:
				c.handleRequestVote(req)
			case <-timer.C:
				c.logger.Info("Unable to acquire necessary votes [%v/%v]", numVotes, needed)
				c.ctrl.Close()
				becomeFollower(c.replica)
				return
			case resp := <-ballots:
				if resp.Err != nil {
					c.logger.Info("Error retrieving vote from peer [%v]: %v", resp.Peer, resp.Err)
					continue
				}

				vote := resp.Val.(VoteResponse)
				if vote.Term > c.term.Epoch {
					c.logger.Info("A later term was detected [%v]", vote.Term)
					c.ctrl.Close()
					c.replica.SetTerm(vote.Term, nil, nil)
					becomeFollower(c.replica)
					return
				}

				if vote.Granted {
					numVotes++
				}
			}
		}
	}()
}

func (c *candidate) handleAppend(req *chans.Request) {
	req.Fail(ErrNotLeader)
}

func (c *candidate) handleReadBarrier(req *chans.Request) {
	req.Fail(ErrNotLeader)
}

func (c *candidate) handleRosterUpdate(req *chans.Request) {
	req.Fail(ErrNotLeader)
}

func (c *candidate) handleRequestVote(req *chans.Request) {
	vote := req.Body().(VoteRequest)

	c.logger.Debug("Handling vote request [term=%v,peer=%v]", vote.Term, vote.Id.String()[:8])
	if vote.Term <= c.term.Epoch { // we can only vote once during an election
		req.Ack(VoteResponse{Term: c.term.Epoch, Granted: false})
		return
	}

	// If the replica isn't recognized, deny the vote.
	_, ok := c.replica.FindPeer(vote.Id)
	if !ok {
		req.Ack(VoteResponse{Term: vote.Term, Granted: false})
		return
	}

	// If the voting peer's log is at least as new as this
	// candidates log, then we will give our vote.
	maxIndex, maxTerm, err := c.replica.Log.LastIndexAndTerm()
	if err != nil {
		req.Fail(err)
		return
	}

	if vote.MaxLogTerm >= maxTerm && vote.MaxLogIndex >= maxIndex {
		c.logger.Debug("Voting for candidate [%v]", vote.Id.String()[:8])
		c.replica.SetTerm(vote.Term, nil, &vote.Id)
		c.ctrl.Close()
		becomeFollower(c.replica)
		req.Ack(VoteResponse{Term: vote.Term, Granted: true}) // should this go after the control has been closed??
		return
	}

	c.logger.Debug("Rejecting candidate vote [%v]", vote.Id.String())
	c.replica.SetTerm(vote.Term, nil, nil)
	c.ctrl.Close()
	req.Ack(VoteResponse{Term: vote.Term, Granted: false})
	becomeFollower(c.replica)
}

func (c *candidate) handleReplication(req *chans.Request) {
	repl := req.Body().(ReplicateRequest)
	if repl.Term < c.term.Epoch {
		req.Ack(ReplicateResponse{Term: c.term.Epoch, Success: false})
		return
	}

	// We're going to do a simple optimized agreement.  We fail
	// but hint at what the proper log entry should look like.
	// The new leader should take the hint and seek to that
	// index to start replication.  By that time we'll be a
	// follower.
	max, _, err := c.replica.Log.LastIndexAndTerm()
	if err != nil {
		req.Fail(err)
		return
	}

	// repl.term is >= term.  use it from now on.
	c.logger.Info("Detected new leader [%v]", repl.LeaderId.String()[:8])
	c.replica.SetTerm(repl.Term, &repl.LeaderId, &repl.LeaderId)
	c.ctrl.Close()
	req.Ack(ReplicateResponse{Term: repl.Term, Success: false, Hint: max})
	becomeFollower(c.replica)
}
