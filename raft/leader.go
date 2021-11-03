package raft

import (
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/chans"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkopriv2/golang-sdk/lang/pool"
	uuid "github.com/satori/go.uuid"
)

type leader struct {
	ctx      context.Context
	logger   context.Logger
	ctrl     context.Control
	syncer   *logSyncer
	workPool pool.WorkPool
	term     term
	replica  *replica
}

func becomeLeader(replica *replica) {
	replica.SetTerm(replica.CurrentTerm().Num, &replica.Self.Id, &replica.Self.Id)

	ctx := replica.Ctx.Sub("Leader(%v)", replica.CurrentTerm())
	ctx.Logger().Info("Becoming leader")

	workPool := pool.NewWorkPool(ctx.Control(), 20)
	ctx.Control().Defer(func(error) {
		workPool.Close()
	})

	logSyncer := newLogSyncer(ctx, replica)
	ctx.Control().Defer(func(error) {
		logSyncer.Close()
	})

	l := &leader{
		ctx:      ctx,
		logger:   ctx.Logger(),
		ctrl:     ctx.Control(),
		syncer:   logSyncer,
		workPool: workPool,
		term:     replica.CurrentTerm(),
		replica:  replica,
	}

	l.start()
}

func (l *leader) start() {
	// Roster routine
	go func() {
		defer l.ctrl.Close()

		for !l.ctrl.IsClosed() {
			select {
			case <-l.ctrl.Closed():
				return
			case req := <-l.replica.RosterUpdates:
				l.handleRosterUpdate(req)
			}
		}
	}()

	// Main routine
	go func() {
		defer l.ctrl.Close()

		timer := time.NewTimer(l.replica.ElectionTimeout / 5)
		defer timer.Stop()
		for !l.ctrl.IsClosed() {

			select {
			case <-l.ctrl.Closed():
				return
			case <-l.replica.ctrl.Closed():
				return
			case req := <-l.replica.RemoteAppends:
				l.handleLocalAppend(req)
			case req := <-l.replica.LocalAppends:
				l.handleLocalAppend(req)
			case req := <-l.replica.Snapshots:
				l.handleInstallSnapshot(req)
			case req := <-l.replica.Replications:
				l.handleReplication(req)
			case req := <-l.replica.VoteRequests:
				l.handleRequestVote(req)
			case req := <-l.replica.Barrier:
				l.handleReadBarrier(req)
			case <-timer.C:
				l.broadcastHeartbeat()
			case <-l.syncer.ctrl.Closed():
				l.logger.Error("Syncer closed: %v", l.syncer.ctrl.Failure())
				l.ctrl.Close()
				becomeFollower(l.replica)
				return
			}

			l.logger.Debug("Resetting timeout [%v]", l.replica.ElectionTimeout/5)
			timer.Reset(l.replica.ElectionTimeout / 5)
		}
	}()

	// Establish leadership
	if !l.broadcastHeartbeat() {
		l.ctrl.Close()
		becomeFollower(l.replica)
		return
	}

	// Establish read barrier
	if _, err := l.replica.LocalAppend(appendEventRequest{Event{}, NoOp}); err != nil {
		l.ctrl.Close()
		becomeFollower(l.replica)
		return
	}
}

// leaders do not accept snapshot installations
func (c *leader) handleInstallSnapshot(req *chans.Request) {
	snapshot := req.Body().(installSnapshotRequest)
	if snapshot.Term <= c.term.Num {
		req.Ack(installSnapshotResponse{Term: c.term.Num, Success: false})
		return
	}

	c.replica.SetTerm(snapshot.Term, &snapshot.LeaderId, &snapshot.LeaderId)
	req.Ack(installSnapshotResponse{Term: snapshot.Term, Success: false})
	becomeFollower(c.replica)
	c.ctrl.Close()
}

// leaders do not accept replication requests
func (c *leader) handleReplication(req *chans.Request) {
	repl := req.Body().(replicateRequest)
	if repl.Term <= c.term.Num {
		req.Ack(replicateResponse{Term: c.term.Num, Success: false})
		return
	}

	c.replica.SetTerm(repl.Term, &repl.LeaderId, &repl.LeaderId)
	req.Ack(replicateResponse{Term: repl.Term, Success: false})
	becomeFollower(c.replica)
	c.ctrl.Close()
}

func (c *leader) handleReadBarrier(req *chans.Request) {
	if c.broadcastHeartbeat() {
		req.Ack(c.replica.Log.Committed())
		return
	}

	req.Fail(ErrNotLeader)
	becomeFollower(c.replica)
	c.ctrl.Close()
}

func (c *leader) handleLocalAppend(req *chans.Request) {
	err := c.workPool.SubmitOrCancel(req.Canceled(), func() {
		req.Return(c.syncer.Append(req.Body().(appendEventRequest)))
		//c.broadcastHeartbeat() // This commits the log entry.
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting work to append pool."))
	}
}

func (c *leader) handleRosterUpdate(req *chans.Request) {
	update := req.Body().(rosterUpdateRequest)

	all := c.replica.Cluster()
	if !update.Join {
		all = all.Delete(update.Peer)
		c.replica.Roster.Set(all)

		bytes, err := Config{all}.encode(enc.Json)
		if err != nil {
			return
		}

		c.logger.Info("Removing peer: %v", update.Peer)
		if _, e := c.replica.Append(bytes, Conf); e != nil {
			req.Fail(e)
			return
		} else {
			req.Ack(true)
			return
		}
	}

	var err error
	all = all.Add(update.Peer)
	c.replica.Roster.Set(all)
	defer func() {
		if err != nil {
			c.replica.Roster.Set(all.Delete(update.Peer))
		}
	}()

	c.syncer.handleRosterChange(all)
	defer func() {
		if err != nil {
			c.syncer.handleRosterChange(all.Delete(update.Peer))
		}
	}()

	sync := c.syncer.GetSyncer(update.Peer.Id)
	defer func() {
		if err != nil {
			sync.Close() // this really isn't necessary, but shouldn't hurt
		}
	}()

	_, err = sync.heartbeat(req.Canceled())
	if err != nil {
		req.Fail(err)
		return
	}

	//score, err := sync.score(req.Canceled())
	//if err != nil {
	//req.Fail(err)
	//return
	//}

	//if score < 0 {
	//req.Fail(errors.Wrapf(ErrTooSlow, "Unable to merge peer: %v", update.Peer))
	//return
	//}

	bytes, err := Config{all}.encode(enc.Json)
	if err != nil {
		req.Fail(err)
		return
	}

	c.logger.Info("Joining peer [%v]", update.Peer)
	if _, e := c.replica.Append(bytes, Conf); e != nil {
		req.Fail(e)
		return
	} else {
		req.Ack(true)
		return
	}
}

func (c *leader) handleRequestVote(req *chans.Request) {
	vote := req.Body().(voteRequest)

	c.logger.Debug("Handling request vote: %v", vote)

	// previous or current term vote.  (immediately decline.  already leader)
	if vote.Term <= c.term.Num {
		req.Ack(voteResponse{Term: c.term.Num, Granted: false})
		return
	}

	// deny the vote if we don't know about the peer.
	_, ok := c.replica.FindPeer(vote.Id)
	if !ok {
		req.Ack(voteResponse{Term: vote.Term, Granted: false})
		c.replica.SetTerm(vote.Term, nil, nil)
		c.ctrl.Close()
		becomeCandidate(c.replica)
		return
	}

	// future term vote.  (move to new term.  only accept if candidate log is long enough)
	maxIndex, maxTerm, err := c.replica.Log.LastIndexAndTerm()
	if err != nil {
		req.Ack(voteResponse{Term: vote.Term, Granted: false})
		return
	}

	if vote.MaxLogIndex >= maxIndex && vote.MaxLogTerm >= maxTerm {
		c.replica.SetTerm(vote.Term, nil, &vote.Id)
		req.Ack(voteResponse{Term: vote.Term, Granted: true})
	} else {
		c.replica.SetTerm(vote.Term, nil, nil)
		req.Ack(voteResponse{Term: vote.Term, Granted: false})
	}

	c.ctrl.Close()
	becomeFollower(c.replica)
}

func (c *leader) broadcastHeartbeat() bool {
	syncers := c.syncer.Syncers()
	ch := make(chan replicateResponse, len(syncers))
	for _, p := range syncers {
		go func(p *peerSyncer) {
			resp, err := p.heartbeat(c.ctrl.Closed())
			if err != nil {
				ch <- replicateResponse{Term: c.term.Num, Success: false}
			} else {
				ch <- resp
			}
		}(p)
	}

	timer := time.NewTimer(c.replica.ElectionTimeout)
	defer timer.Stop()
	for i := 0; i < c.replica.Majority()-1; {
		select {
		case <-c.ctrl.Closed():
			return false
		case resp := <-ch:
			if resp.Term > c.term.Num {
				c.replica.SetTerm(resp.Term, nil, nil)
				c.ctrl.Close()
				becomeFollower(c.replica)
				return false
			}

			i++
		case <-timer.C:
			c.logger.Error("Unable to retrieve enough heartbeat responses.")
			c.replica.SetTerm(c.term.Num, nil, c.term.VotedFor)
			c.ctrl.Close()
			becomeFollower(c.replica)
			return false
		}
	}

	return true
}

// the log syncer should be rebuilt every time a leader comes to power.
type logSyncer struct {

	// the context (injected by parent and spawned)
	ctx context.Context

	// the logger (injected by parent.  do not use root's logger)
	logger context.Logger

	// the core syncer lifecycle
	ctrl context.Control

	// the primary replica instance. ()
	self *replica

	// the current term (extracted because the sync'er needs consistent view of term)
	term term

	// used to determine peer sync state
	syncers map[uuid.UUID]*peerSyncer

	// Used to access/update peer states.
	syncersLock sync.Mutex
}

func newLogSyncer(ctx context.Context, self *replica) *logSyncer {
	ctx = ctx.Sub("Syncer")

	s := &logSyncer{
		ctx:     ctx,
		logger:  ctx.Logger(),
		ctrl:    ctx.Control(),
		self:    self,
		term:    self.CurrentTerm(),
		syncers: make(map[uuid.UUID]*peerSyncer),
	}

	s.start()
	return s
}

func (l *logSyncer) Close() error {
	return l.ctrl.Close()
}

func (s *logSyncer) spawnSyncer(p Peer) *peerSyncer {
	sync := newPeerSyncer(s.ctx, s.self, s.term, p)
	go func() {
		select {
		case <-sync.ctrl.Closed():
			if errs.Is(sync.ctrl.Failure(), ErrNotLeader) {
				s.logger.Info("No longer leader. Shutting down")
				s.ctrl.Fail(ErrNotLeader)
				return
			}

			select {
			case <-s.ctrl.Closed():
				return
			case <-time.After(500 * time.Millisecond):
			}

			s.logger.Info("Syncer [%v] closed: %v", p, sync.ctrl.Failure())
			s.handleRosterChange(s.self.Cluster())
			return
		case <-s.ctrl.Closed():
			return
		}
	}()
	return sync
}

func (s *logSyncer) handleRosterChange(peers []Peer) {
	cur, active := s.Syncers(), make(map[uuid.UUID]*peerSyncer)

	// Add any missing
	for _, p := range peers {
		if p.Id == s.self.Self.Id {
			continue
		}

		if sync, ok := cur[p.Id]; ok && !sync.ctrl.IsClosed() {
			active[p.Id] = sync
			continue
		}

		active[p.Id] = s.spawnSyncer(p)
	}

	// Remove any missing
	for id, sync := range cur {
		if _, ok := active[id]; !ok {
			sync.Close()
		}
	}

	s.SetSyncers(active)
}

func (s *logSyncer) start() {
	peers, ver := s.self.Roster.Get()
	s.handleRosterChange(peers)

	var ok bool
	go func() {
		for {
			peers, ver, ok = s.self.Roster.Wait(ver)
			if s.ctrl.IsClosed() || !ok {
				return
			}
			s.handleRosterChange(peers)
		}
	}()
}

func (s *logSyncer) Append(req appendEventRequest) (Entry, error) {
	committed := make(chan Entry, 1)
	go func() {
		// append
		item, err := s.self.Log.Append(req.Event, s.term.Num, req.Kind)
		if err != nil {
			s.ctrl.Fail(err)
			return
		}

		// wait for majority to append as well
		majority := s.self.Majority() - 1
		for done := make(map[uuid.UUID]struct{}); len(done) < majority; {
			for _, p := range s.self.Others() {
				if _, ok := done[p.Id]; ok {
					continue
				}

				syncer := s.GetSyncer(p.Id)
				if syncer == nil {
					continue
				}

				index, term := syncer.GetPrevIndexAndTerm()
				if index >= item.Index && term >= s.term.Num {
					done[p.Id] = struct{}{}
				}
			}

			if s.ctrl.IsClosed() {
				return
			}

			time.Sleep(10 * time.Millisecond)
		}

		s.self.Log.Commit(item.Index) // commutative, so safe in the event of out of order commits.
		committed <- item
	}()

	select {
	case <-s.ctrl.Closed():
		return Entry{}, errs.Or(s.ctrl.Failure(), ErrClosed)
	case item := <-committed:
		return item, nil
	}
}

func (s *logSyncer) GetSyncer(id uuid.UUID) *peerSyncer {
	s.syncersLock.Lock()
	defer s.syncersLock.Unlock()
	return s.syncers[id]
}

func (s *logSyncer) Syncers() map[uuid.UUID]*peerSyncer {
	s.syncersLock.Lock()
	defer s.syncersLock.Unlock()
	ret := make(map[uuid.UUID]*peerSyncer)
	for k, v := range s.syncers {
		ret[k] = v
	}
	return ret
}

func (s *logSyncer) SetSyncers(syncers map[uuid.UUID]*peerSyncer) {
	s.syncersLock.Lock()
	defer s.syncersLock.Unlock()
	s.syncers = syncers
}

// a peer syncer is responsible for sync'ing a single peer's log.
type peerSyncer struct {
	logger    context.Logger
	ctrl      context.Control
	peer      Peer
	term      term
	self      *replica
	prevIndex int64
	prevTerm  int64
	prevLock  sync.RWMutex
	pool      pool.ObjectPool // T: *rpcClient
}

func newPeerSyncer(ctx context.Context, self *replica, term term, peer Peer) *peerSyncer {
	sub := ctx.Sub("Sync(%v)", peer)

	pool := peer.ClientPool(sub.Control(), self.Options)
	sub.Control().Defer(func(error) {
		pool.Close()
	})
	sync := &peerSyncer{
		logger:    sub.Logger(),
		ctrl:      sub.Control(),
		self:      self,
		peer:      peer,
		term:      term,
		prevIndex: -1,
		prevTerm:  -1,
		pool:      pool,
	}
	sync.start()
	return sync
}

func (s *peerSyncer) Close() error {
	return s.ctrl.Close()
}

func (l *peerSyncer) GetPrevIndexAndTerm() (int64, int64) {
	l.prevLock.RLock()
	defer l.prevLock.RUnlock()
	return l.prevIndex, l.prevTerm
}

func (l *peerSyncer) SetPrevIndexAndTerm(index int64, term int64) {
	l.prevLock.Lock()
	defer l.prevLock.Unlock()
	l.prevIndex = index
	l.prevTerm = term
}

func (s *peerSyncer) send(cancel <-chan struct{}, fn func(cl *rpcClient) error) error {
	raw, err := s.pool.TakeOrCancel(cancel)
	if err != nil {
		return err
	}

	if err := fn(raw.(*rpcClient)); err != nil {
		s.pool.Fail(raw)
		return err
	} else {
		s.pool.Return(raw)
		return nil
	}
}

func (s *peerSyncer) heartbeat(cancel <-chan struct{}) (resp replicateResponse, err error) {
	err = s.send(cancel, func(cl *rpcClient) error {
		resp, err = cl.Replicate(newHeartBeat(s.self.Self.Id, s.term.Num, s.self.Log.Committed()))
		return err
	})
	return
}

// Per raft: A leader never overwrites or deletes entries in its log; it only appends new entries. §3.5
// no need to worry about truncations here...however, we do need to worry about compactions interrupting
// syncing.
func (s *peerSyncer) start() {
	s.logger.Info("Starting")
	go func() {
		defer s.ctrl.Close()
		defer s.logger.Info("Shutting down")

		prev, err := s.getLatestLocalEntry() // is there a better way to initialize this??
		if err != nil {
			s.ctrl.Fail(err)
			return
		}

		for {
			next, ok := s.self.Log.head.WaitUntil(prev.Index + 1)
			if !ok || s.ctrl.IsClosed() {
				return
			}

			// loop until this peer is completely caught up to head!
			for prev.Index < next {
				if s.ctrl.IsClosed() {
					return
				}

				// might have to reinitialize client after each batch.
				s.logger.Info("Position [%v/%v]", prev.Index, next)
				err := s.send(s.ctrl.Closed(), func(cl *rpcClient) error {
					prev, ok, err = s.sendBatch(cl, prev, next)
					if err != nil {
						return errors.Wrapf(err, "Error sending batch [prev=%v,next=%v]", prev.Index, next)
					}

					if ok {
						s.SetPrevIndexAndTerm(prev.Index, prev.Term)
						return nil
					}

					s.logger.Info("Too far behind [index=%v]. Installing snapshot.", prev.Index)

					prev, err = s.sendSnapshotToClient(cl)
					if err != nil {
						return errors.Wrap(err, "Error sending snapshot")
					}

					s.SetPrevIndexAndTerm(prev.Index, prev.Term)
					return nil
				})
				if err != nil {
					s.ctrl.Fail(err)
					return
				}
			}

			s.logger.Debug("Synced to [%v]", prev.Index)
		}
	}()
}

func (s *peerSyncer) score(cancel <-chan struct{}) (int64, error) {

	// delta just calulcates distance from sync position to max
	delta := func() (int64, error) {
		if s.ctrl.IsClosed() {
			return 0, errs.Or(ErrClosed, s.ctrl.Failure())
		}

		max, _, err := s.self.Log.LastIndexAndTerm()
		if err != nil {
			return 0, err
		}

		idx, _ := s.GetPrevIndexAndTerm()
		return max - idx, nil
	}

	// watch the sync'er.
	prevDelta := int64(math.MaxInt64)

	score := int64(0)
	for rounds := int64(0); rounds < 30; rounds++ {
		s.heartbeat(cancel)

		curDelta, err := delta()
		if err != nil {
			return 0, err
		}

		// This is totally arbitrary.
		if curDelta < 2 && score >= 1 {
			break
		}

		if curDelta < 8 && score >= 3 {
			break
		}

		if curDelta < 128 && score >= 4 {
			break
		}

		if curDelta < 1024 && score >= 5 {
			break
		}

		if curDelta <= prevDelta {
			score++
		} else {
			score--
		}

		s.logger.Info("Delta [%v] after [%v] rounds.  Score: [%v]", curDelta, rounds+1, score)
		time.Sleep(s.self.ElectionTimeout / 5)
		prevDelta = curDelta
	}

	return score, nil
}

// returns the starting position for syncing a newly initialized sync'er
func (s *peerSyncer) getLatestLocalEntry() (Entry, error) {
	lastIndex, lastTerm, err := s.self.Log.LastIndexAndTerm()
	if err != nil {
		return Entry{}, err
	}

	prev, ok, err := s.self.Log.Get(lastIndex - 1) // probably shouldn't do it like this
	if ok || err != nil {
		return prev, err
	}

	return Entry{Index: lastIndex, Term: lastTerm}, nil
}

// Sends a batch up to the horizon
func (s *peerSyncer) sendBatch(cl *rpcClient, prev Entry, horizon int64) (Entry, bool, error) {
	// scan a full batch of events.
	beg, end := prev.Index+1, min(horizon+1, prev.Index+1+256)

	batch, err := s.self.Log.Scan(beg, end)
	if err != nil || len(batch) == 0 {
		return prev, false, err
	}

	s.logger.Debug("Sending batch [offset=%v, num=%v]", batch[0].Index, len(batch))

	// send the append request.
	resp, err := cl.Replicate(newReplication(s.self.Self.Id, s.term.Num, prev.Index, prev.Term, batch, s.self.Log.Committed()))
	if err != nil {
		return prev, false, errors.Wrapf(err, "Error replicating batch [prev=%v,num=%v]", prev.Index, len(batch))
	}

	// make sure we're still a leader.
	if resp.Term > s.term.Num {
		return prev, false, ErrNotLeader
	}

	// if it was successful, progress the peer's index and term
	if resp.Success {
		return batch[len(batch)-1], true, nil
	}

	s.logger.Error("Consistency check failed. Received hint [%v]", resp.Hint)
	prev, ok, err := s.self.Log.Get(min(resp.Hint, prev.Index-1))
	return prev, ok, err
}

func (s *peerSyncer) sendSnapshotToClient(cl *rpcClient) (Entry, error) {
	snapshot, err := s.self.Log.Snapshot()
	if err != nil {
		return Entry{}, err
	}

	err = s.sendSnapshot(cl, snapshot)
	if err != nil {
		return Entry{}, err
	}

	return Entry{Index: snapshot.LastIndex(), Term: snapshot.LastTerm()}, nil
}

// sends the snapshot to the client
func (l *peerSyncer) sendSnapshot(cl *rpcClient, snapshot StoredSnapshot) error {
	size := snapshot.Size()
	sendSegment := func(cl *rpcClient, offset int64, batch []Event) error {
		segment := installSnapshotRequest{
			LeaderId:    l.self.Self.Id,
			Id:          snapshot.Id(),
			Term:        l.term.Num,
			Config:      snapshot.Config(),
			Size:        size,
			MaxIndex:    snapshot.LastIndex(),
			MaxTerm:     snapshot.LastTerm(),
			BatchOffset: offset,
			Batch:       batch}

		resp, err := cl.InstallSnapshotSegment(segment)
		if err != nil {
			return err
		}

		if resp.Term > l.term.Num || !resp.Success {
			return ErrNotLeader
		}

		return nil
	}

	if size == 0 {
		return sendSegment(cl, 0, []Event{})
	}

	for i := int64(0); i < size; {
		if l.ctrl.IsClosed() {
			return ErrClosed
		}

		beg, end := i, min(size, i+256)
		l.logger.Info("Sending snapshot segment [%v,%v]", beg, end)

		batch, err := snapshot.Scan(beg, end)
		if err != nil {
			return errors.Wrapf(err, "Error scanning batch [%v, %v]", beg, end)
		}

		err = sendSegment(cl, beg, batch)
		if err != nil {
			return errors.Wrapf(err, "Error sending batch [%v, %v]", beg, end)
		}

		i += int64(len(batch))
	}

	return nil
}
