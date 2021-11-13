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

var (
	ErrConnRefused = errors.New("connection refused")
)

// This implements the leader state machine and all accompanying services.
// Because this is steady-state machine, it must implement all of the requests
// types.  There are cases where clients are making requests that only followers
// respond to.  In those cases, these clients believe the leader is a follower.
// If this leader is legitimate, it continutes on, otherwise it becomes a
// follower.
//
// This also contains the implementations for the log-syncing routine.  That
// routine manages a set of individual peer syncers, where there is a single
// peer syncer per peer.  The log syncer is responsible for appending entries
// to the underlying log and committing entries as majorities are discovered.
//
// The leader reads requests from the replica instance and processes to them.
type leader struct {
	ctx      context.Context
	logger   context.Logger
	ctrl     context.Control
	syncer   *logSyncer
	workPool pool.WorkPool
	term     Term
	replica  *replica
}

func becomeLeader(replica *replica) {
	replica.SetTerm(replica.CurrentTerm().Epoch, &replica.Self.Id, &replica.Self.Id)

	// We need a new root context so we don't leak defers on the replica's context.
	// Basically, anything that is part of the leader's lifecycle needs to be its
	// own isolated object hierarchy.
	ctx := replica.NewRootContext().Sub("Peer(%v): Leader(%v)", replica.Self, replica.CurrentTerm())
	ctx.Logger().Info("Becoming leader")

	logSyncer := newLogSyncer(ctx, replica)
	l := &leader{
		ctx:      ctx,
		logger:   ctx.Logger(),
		ctrl:     ctx.Control(),
		syncer:   logSyncer,
		workPool: pool.NewWorkPool(ctx.Control(), replica.Options.MaxWorkers),
		term:     replica.CurrentTerm(),
		replica:  replica,
	}

	l.start()
}

func (l *leader) start() {

	// Establish leadership
	if !tryBroadCastHeartbeat(l.syncer.Syncers(), l.ctrl.Closed()) {
		l.ctrl.Close()
		becomeFollower(l.replica)
		return
	}

	// Roster routine
	go func() {
		defer l.ctrl.Close()

		for !l.ctrl.IsClosed() {
			select {
			case <-l.ctrl.Closed():
				return
			case req := <-l.replica.RosterUpdateRequests:
				l.handleRosterUpdate(req)
			}
		}
	}()

	// Main routine
	go func() {
		defer l.ctrl.Close()
		defer func() {
			// If the leader is shutting down, it could be because it is leaving the cluster.
			// We need to do a best effort attempt of propagating any pending commits. If we
			// do not, the other peers MAY NOT recognize that the leader is gone and will
			// continue to attempt to connect to it until the config entry is committed.
			tryBroadCastHeartbeat(l.syncer.Syncers(), l.ctrl.Closed())
		}()

		timer := time.NewTimer(l.replica.ElectionTimeout / 5)
		defer timer.Stop()
		for !l.ctrl.IsClosed() {

			select {
			case <-l.ctrl.Closed():
				return
			case <-l.replica.ctrl.Closed():
				return
			case req := <-l.replica.AppendRequests:
				l.handleAppend(req)
			case req := <-l.replica.SnapshotRequests:
				l.handleInstallSnapshot(req)
			case req := <-l.replica.ReplicationRequests:
				l.handleReplication(req)
			case req := <-l.replica.VoteRequests:
				l.handleRequestVote(req)
			case req := <-l.replica.BarrierRequests:
				l.handleReadBarrier(req)
			case <-timer.C:
				tryBroadCastHeartbeat(l.syncer.Syncers(), l.ctrl.Closed())
			case <-l.syncer.ctrl.Closed():
				l.logger.Error("Syncer closed: %v", l.syncer.ctrl.Failure())
				l.ctrl.Close()
				becomeFollower(l.replica)
				return
			}

			//l.logger.Debug("Resetting timeout [%v]", l.replica.ElectionTimeout/5)
			timer.Reset(l.replica.ElectionTimeout / 5)
		}
	}()

}

// leaders do not accept snapshot installations
func (c *leader) handleInstallSnapshot(req *chans.Request) {
	snapshot := req.Body().(InstallSnapshotRequest)
	if snapshot.Term <= c.term.Epoch {
		req.Ack(InstallSnapshotResponse{Term: c.term.Epoch, Success: false})
		return
	}

	c.replica.SetTerm(snapshot.Term, &snapshot.LeaderId, &snapshot.LeaderId)
	req.Ack(InstallSnapshotResponse{Term: snapshot.Term, Success: false})
	c.ctrl.Close()
	becomeFollower(c.replica)
}

// leaders do not accept replication requests
func (c *leader) handleReplication(req *chans.Request) {
	repl := req.Body().(ReplicateRequest)
	if repl.Term <= c.term.Epoch {
		req.Ack(ReplicateResponse{Term: c.term.Epoch, Success: false})
		return
	}

	c.replica.SetTerm(repl.Term, &repl.LeaderId, &repl.LeaderId)
	req.Ack(ReplicateResponse{Term: repl.Term, Success: false})
	c.ctrl.Close()
	becomeFollower(c.replica)
}

func (c *leader) handleReadBarrier(req *chans.Request) {
	req.Ack(ReadBarrierResponse{c.replica.Log.Committed()})
	return
}

func (c *leader) handleAppend(req *chans.Request) {
	err := c.workPool.SubmitOrCancel(req.Canceled(), func() {
		req.Return(c.syncer.Append(req.Canceled(), req.Body().(AppendRequest)))
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting work to append pool."))
	}
}

func (c *leader) handleRosterUpdate(req *chans.Request) {
	update := req.Body().(RosterUpdateRequest)

	all := c.replica.Cluster()
	if !update.Join {
		c.logger.Info("Removing peer: %v", update.Peer)

		bytes, err := Config{all.Delete(update.Peer)}.encode(enc.Json)
		if err != nil {
			req.Fail(err)
			return
		}

		if _, err = c.replica.Append(AppendRequest{bytes, Conf}); err != nil {
			c.logger.Error("Error removing peer: %v", err)
		}

		req.Fail(err)
		return
	}

	c.logger.Info("Adding peer [%v]", update.Peer)

	var err error
	all = all.Add(update.Peer)

	// Speculatively add the host to start syncing.  This isn't completely safe
	// because there is a routine that is checking for changes to the roster
	// and then creating and removing syncers.
	c.syncer.handleRosterChange(all)
	defer func() {
		if err != nil {
			c.syncer.handleRosterChange(all.Delete(update.Peer))
		}
	}()

	sync, ok := c.syncer.GetSyncer(update.Peer.Id)
	if !ok {
		req.Fail(errors.Wrapf(ErrInvalid, "Failed to start peer syncer [%v]", update.Peer))
		return
	}
	defer func() {
		if err != nil {
			sync.Close() // this really isn't necessary, but shouldn't hurt
		}
	}()

	_, err = sync.Heartbeat(req.Canceled())
	if err != nil {
		req.Fail(err)
		return
	}

	score, err := sync.Score(req.Canceled())
	if err != nil {
		req.Fail(err)
		return
	}

	if score < 0 {
		req.Fail(errors.Wrapf(ErrTooSlow, "Unable to merge peer: %v", update.Peer))
		return
	}

	bytes, err := Config{all}.encode(enc.Json)
	if err != nil {
		req.Fail(err)
		return
	}

	_, err = c.replica.Append(AppendRequest{bytes, Conf})
	req.Fail(err)
}

func (c *leader) handleRequestVote(req *chans.Request) {
	vote := req.Body().(VoteRequest)
	c.logger.Debug("Handling request vote [term=%v,peer=%v]", vote.Term, vote.Id.String()[:8])

	// previous or current term vote.  (immediately decline.  already leader)
	if vote.Term <= c.term.Epoch {
		req.Ack(VoteResponse{Term: c.term.Epoch, Granted: false})
		return
	}

	// deny the vote if we don't know about the peer.
	_, ok := c.replica.FindPeer(vote.Id)
	if !ok {
		req.Ack(VoteResponse{Term: vote.Term, Granted: false})
		c.replica.SetTerm(vote.Term, nil, nil)
		c.ctrl.Close()
		becomeCandidate(c.replica)
		return
	}

	// future term vote.  (move to new term.  only accept if candidate log is long enough)
	maxIndex, maxTerm, err := c.replica.Log.LastIndexAndTerm()
	if err != nil {
		req.Ack(VoteResponse{Term: vote.Term, Granted: false})
		return
	}

	if vote.MaxLogIndex >= maxIndex && vote.MaxLogTerm >= maxTerm {
		c.replica.SetTerm(vote.Term, nil, &vote.Id)
		req.Ack(VoteResponse{Term: vote.Term, Granted: true})
	} else {
		c.replica.SetTerm(vote.Term, nil, nil)
		req.Ack(VoteResponse{Term: vote.Term, Granted: false})
	}

	c.ctrl.Close()
	becomeFollower(c.replica)
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
	term Term

	// used to determine peer sync state
	syncers map[uuid.UUID]*peerSyncer

	// Used to access/update peer states.
	syncersLock sync.Mutex
}

func newLogSyncer(ctx context.Context, self *replica) *logSyncer {
	ctx = ctx.Sub("LogSyncer")

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
				s.ctrl.Fail(ErrNotLeader)
				return
			}

			sleep := 500 * time.Millisecond
			if errs.Is(sync.ctrl.Failure(), ErrConnRefused) {
				sleep = 30 * time.Second
			}

			s.logger.Info("Syncer [%v] closed: %v", p, sync.ctrl.Failure())
			select {
			case <-s.ctrl.Closed():
				return
			case <-time.After(sleep):
			}

			s.handleRosterChange(s.self.Cluster())
			return
		case <-s.ctrl.Closed():
			return
		}
	}()
	return sync
}

func (s *logSyncer) handleRosterChange(peers Peers) {
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
			s.logger.Info("New roster: %v", peers.Flatten())
			s.handleRosterChange(peers)
		}
	}()
}

func (s *logSyncer) Append(cancel <-chan struct{}, req AppendRequest) (entry Entry, err error) {
	entry, err = s.self.Log.Append(req.Event, s.term.Epoch, req.Kind)
	if err != nil {
		s.ctrl.Fail(err)
		return
	}

	// wait for majority to append as well
	for done := make(map[uuid.UUID]struct{}); ; {
		cluster := s.self.Cluster()
		if len(done) >= majority(len(cluster))-1 {
			break
		}

		for _, p := range cluster {
			if _, ok := done[p.Id]; ok {
				continue
			}

			sync, ok := s.GetSyncer(p.Id)
			if !ok {
				continue
			}

			index, term := sync.GetPrevIndexAndTerm()
			if index >= entry.Index && term >= entry.Term {
				done[sync.peer.Id] = struct{}{}
			}
		}

		timer := time.NewTimer(10 * time.Microsecond)
		select {
		case <-s.ctrl.Closed():
			timer.Stop()
			err = ErrClosed
			return
		case <-cancel:
			timer.Stop()
			err = ErrCanceled
			return
		case <-timer.C:
			timer.Stop()
		}
	}

	s.logger.Debug("Committing index: %v", entry.Index)
	s.self.Log.Commit(entry.Index) // commutative, so safe in the event of out of order commits.
	return
}

func (s *logSyncer) GetSyncer(id uuid.UUID) (ret *peerSyncer, ok bool) {
	s.syncersLock.Lock()
	defer s.syncersLock.Unlock()
	ret, ok = s.syncers[id]
	return
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
	term      Term
	self      *replica
	prevIndex int64
	prevTerm  int64
	prevLock  sync.RWMutex
	pool      pool.ObjectPool // T: *Client
}

func newPeerSyncer(ctx context.Context, self *replica, term Term, peer Peer) *peerSyncer {
	sub := ctx.Sub("PeerSync(%v)", peer)

	sync := &peerSyncer{
		logger:    sub.Logger(),
		ctrl:      sub.Control(),
		self:      self,
		peer:      peer,
		term:      term,
		prevIndex: -1,
		prevTerm:  -1,

		// We unfortunately can't use the client pool on the replica.  There are cases
		// where the replica shuts down before the leader can send out a final heartbeat.
		// If a leader leaves a cluster of two nodes, the remaining peer would not
		// be able to make progress in any election because the commit for the config
		// entry that removed the leader would never happen.
		pool: peer.ClientPool(sub.Control(), self.Options),
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

func (l *peerSyncer) setPrevIndexAndTerm(index int64, term int64) {
	l.prevLock.Lock()
	defer l.prevLock.Unlock()
	l.prevIndex = index
	l.prevTerm = term
}

func (s *peerSyncer) Send(cancel <-chan struct{}, fn func(cl *Client) error) error {
	raw, err := s.pool.TakeOrCancel(cancel)
	if err != nil {
		return err
	}

	if err := fn(raw.(*Client)); err != nil {
		s.pool.Fail(raw)
		return err
	} else {
		s.pool.Return(raw)
		return nil
	}
}

func (s *peerSyncer) Heartbeat(cancel <-chan struct{}) (resp ReplicateResponse, err error) {
	err = s.Send(cancel, func(cl *Client) error {
		resp, err = cl.Replicate(newHeartBeat(s.self.Self.Id, s.term.Epoch, s.self.Log.Committed()))
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

			// loop until this peer is completely caught up to next
			for prev.Index < next {
				if s.ctrl.IsClosed() {
					return
				}

				s.logger.Debug("Lag [%v]", next-prev.Index)

				cl, err := s.pool.TakeOrCancel(s.ctrl.Closed())
				if err != nil {
					s.ctrl.Fail(err)
					return
				}

				prev, ok, err = s.sendBatch(cl.(*Client), prev, next)
				if err != nil {
					s.pool.Fail(cl)
					s.ctrl.Fail(err)
					return
				}

				if ok {
					s.pool.Return(cl)
					s.logger.Debug("Synced to [%v]", prev.Index)
					s.setPrevIndexAndTerm(prev.Index, prev.Term)
					continue
				}

				s.logger.Debug("Too far behind [index=%v]. Installing snapshot.", prev.Index)
				prev, err = s.sendSnapshotToClient(cl.(*Client))
				if err != nil {
					s.pool.Fail(cl)
					s.ctrl.Fail(err)
					return
				}

				s.pool.Return(cl)
				s.setPrevIndexAndTerm(prev.Index, prev.Term)
			}

		}
	}()
}

func (s *peerSyncer) Score(cancel <-chan struct{}) (int64, error) {

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
		s.Heartbeat(cancel)

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
func (s *peerSyncer) sendBatch(cl *Client, prev Entry, horizon int64) (ret Entry, ok bool, err error) {
	batch, err := s.self.Log.Scan(prev.Index+1, min(horizon+1, prev.Index+1+256))
	if err != nil {
		return
	}
	if len(batch) == 0 { // shouldn't be possible
		ret, ok = prev, true
		return
	}

	s.logger.Debug("Sending batch [offset=%v, num=%v]", batch[0].Index, len(batch))

	resp, err := cl.Replicate(newReplication(s.self.Self.Id, s.term.Epoch, prev.Index, prev.Term, batch, s.self.Log.Committed()))
	if err != nil {
		err = errors.Wrapf(err, "Error replicating batch [prev=%v,num=%v]", prev.Index, len(batch))
		return
	}

	// make sure we're still a leader.
	if resp.Term > s.term.Epoch {
		err = ErrNotLeader
		return
	}

	// if it was successful, progress the peer's index and term
	if resp.Success {
		ret, ok = batch[len(batch)-1], true
		return
	}

	s.logger.Info("Consistency check failed. Received hint [%v]", resp.Hint)
	return s.self.Log.Get(min(resp.Hint, prev.Index-1))
}

func (s *peerSyncer) sendSnapshotToClient(cl *Client) (ret Entry, err error) {
	snapshot, err := s.self.Log.Snapshot()
	if err != nil {
		return
	}

	if err = s.sendSnapshot(cl, snapshot); err != nil {
		return
	}

	ret = Entry{Index: snapshot.LastIndex(), Term: snapshot.LastTerm()}
	return
}

// sends the snapshot to the client
func (l *peerSyncer) sendSnapshot(cl *Client, snapshot DurableSnapshot) error {
	snapshotId := uuid.NewV1() // generate a random snapshot id for safe multi-tenancy in the db
	sendSegment := func(cl *Client, offset int64, batch []Event) error {
		segment := InstallSnapshotRequest{
			LeaderId:    l.self.Self.Id,
			Term:        l.term.Epoch,
			Id:          snapshotId,
			Config:      snapshot.Config(),
			Size:        snapshot.Size(),
			MaxIndex:    snapshot.LastIndex(),
			MaxTerm:     snapshot.LastTerm(),
			BatchOffset: offset,
			Batch:       batch}

		resp, err := cl.InstallSnapshotSegment(segment)
		if err != nil {
			return err
		}

		if resp.Term > l.term.Epoch || !resp.Success {
			return ErrNotLeader
		}

		return nil
	}

	size := snapshot.Size()
	if size == 0 {
		return sendSegment(cl, 0, []Event{})
	}

	for i := int64(0); i < size; {
		if l.ctrl.IsClosed() {
			return ErrClosed
		}

		beg, end := i, min(size, i+256)
		l.logger.Debug("Sending snapshot segment [%v,%v]", beg, end)

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

func tryBroadCastHeartbeat(syncers map[uuid.UUID]*peerSyncer, cancel <-chan struct{}) (ok bool) {
	ch := make(chan ReplicateResponse, len(syncers))
	for _, p := range syncers {
		go func(p *peerSyncer) {
			resp, err := p.Heartbeat(cancel)
			if err != nil {
				ch <- ReplicateResponse{Term: -1, Success: false}
			} else {
				ch <- resp
			}
		}(p)
	}

	for i := 0; i < len(syncers); i++ {
		select {
		case <-cancel:
			return
		case <-ch:
		}
	}

	return true
}
