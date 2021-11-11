package raft

import (
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

	ctx := replica.Ctx.Sub("Candidate(%v)", replica.CurrentTerm())
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
	ballots := c.replica.Broadcast(func(cl *Client) (interface{}, error) {
		return cl.RequestVote(
			VoteRequest{
				Id:          c.replica.Self.Id,
				Term:        c.term.Epoch,
				MaxLogIndex: maxIndex,
				MaxLogTerm:  maxTerm,
			})
	})

	// Currently this is essentially a single-threaded implementation.
	go func() {
		defer c.ctrl.Close()

		// set the election timer.
		c.logger.Info("Setting timer [%v]", c.replica.ElectionTimeout)
		timer := time.NewTimer(c.replica.ElectionTimeout)

		for numVotes := 1; !c.ctrl.IsClosed(); {
			c.logger.Info("Received [%v/%v] votes", numVotes, len(c.replica.Cluster()))

			needed := c.replica.Majority()
			if numVotes >= needed {
				c.logger.Info("Acquired majority [%v] votes.", needed)
				c.replica.SetTerm(c.replica.term.Epoch, &c.replica.Self.Id, &c.replica.Self.Id)
				becomeLeader(c.replica)
				return
			}

			select {
			case <-c.ctrl.Closed():
				return
			case <-c.replica.ctrl.Closed():
				return
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
					c.replica.SetTerm(vote.Term, nil, nil)
					c.ctrl.Close()
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

func (c *candidate) handleRequestVote(req *chans.Request) {
	vote := req.Body().(VoteRequest)

	c.logger.Debug("Handling vote request: %v", vote)
	if vote.Term <= c.term.Epoch {
		req.Ack(VoteResponse{Term: c.term.Epoch, Granted: false})
		return
	}

	// If the replica isn't recognized, deny the vote.
	_, ok := c.replica.FindPeer(vote.Id)
	if !ok {
		req.Ack(VoteResponse{Term: vote.Term, Granted: false})
		c.replica.SetTerm(vote.Term, nil, nil)
		c.ctrl.Close()
		becomeFollower(c.replica)
		return
	}

	maxIndex, maxTerm, err := c.replica.Log.LastIndexAndTerm()
	if err != nil {
		req.Fail(err)
		return
	}

	if vote.MaxLogIndex >= maxIndex && vote.MaxLogTerm >= maxTerm {
		c.logger.Debug("Voting for candidate [%v]", vote.Id.String())
		req.Ack(VoteResponse{Term: vote.Term, Granted: true}) // should this go after the control has been closed??
		c.replica.SetTerm(vote.Term, nil, &vote.Id)
		becomeFollower(c.replica)
		c.ctrl.Close()
		return
	}

	c.logger.Debug("Rejecting candidate vote [%v]", vote.Id.String())
	req.Ack(VoteResponse{Term: vote.Term, Granted: false})
	c.replica.SetTerm(vote.Term, nil, nil)
}

func (c *candidate) handleReplication(req *chans.Request) {
	repl := req.Body().(ReplicateRequest)
	if repl.Term < c.term.Epoch {
		req.Ack(ReplicateResponse{Term: c.term.Epoch, Success: false})
		return
	}

	max, _, err := c.replica.Log.LastIndexAndTerm()
	if err != nil {
		req.Fail(err)
		return
	}

	// repl.term is >= term.  use it from now on.
	req.Ack(ReplicateResponse{Term: repl.Term, Success: false, Hint: max})
	c.replica.SetTerm(repl.Term, &repl.LeaderId, &repl.LeaderId)
	c.ctrl.Close()
	becomeFollower(c.replica)
}
