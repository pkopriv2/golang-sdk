package raft

//type leader struct {
//ctx        context.Context
//logger     context.Logger
//ctrl       context.Control
//syncer     *logSyncer
//proxyPool  context.WorkPool
//appendPool context.WorkPool
//term       term
//replica    *replica
//}

//func becomeLeader(replica *replica) {
//replica.Term(replica.CurrentTerm().Num, &replica.Id, &replica.Id)

//ctx := replica.Ctx.Sub("Leader(%v)", replica.CurrentTerm())
//ctx.Logger().Info("Becoming leader")

//proxyPool := concurrent.NewWorkPool(ctx.Control(), 20)
//ctx.Control().Defer(func(error) {
//proxyPool.Close()
//})

//appendPool := concurrent.NewWorkPool(ctx.Control(), 20)
//ctx.Control().Defer(func(error) {
//appendPool.Close()
//})

//logSyncer := newLogSyncer(ctx, replica)
//ctx.Control().Defer(func(error) {
//logSyncer.Close()
//})

//l := &leader{
//ctx:        ctx,
//logger:     ctx.Logger(),
//ctrl:       ctx.Control(),
//syncer:     logSyncer,
//proxyPool:  proxyPool,
//appendPool: appendPool,
//term:       replica.CurrentTerm(),
//replica:    replica,
//}

//l.start()
//}

//func (l *leader) start() {
//// Proxy routine.
//go func() {
//defer l.ctrl.Close()

//for !l.ctrl.IsClosed() {
//select {
//case <-l.ctrl.Closed():
//return
//case req := <-l.replica.RemoteAppends:
//l.handleRemoteAppend(req)
//}
//}
//}()

//// Roster routine
//go func() {
//defer l.ctrl.Close()

//for !l.ctrl.IsClosed() {
//select {
//case <-l.ctrl.Closed():
//return
//case req := <-l.replica.RosterUpdates:
//l.handleRosterUpdate(req)
//}
//}
//}()

//// Main routine
//go func() {
//defer l.ctrl.Close()

//timer := time.NewTimer(l.replica.ElectionTimeout / 5)
//for !l.ctrl.IsClosed() {

//select {
//case <-l.ctrl.Closed():
//return
//case <-l.replica.ctrl.Closed():
//return
//case req := <-l.replica.LocalAppends:
//l.handleLocalAppend(req)
//case req := <-l.replica.Snapshots:
//l.handleInstallSnapshot(req)
//case req := <-l.replica.Replications:
//l.handleReplication(req)
//case req := <-l.replica.VoteRequests:
//l.handleRequestVote(req)
//case req := <-l.replica.Barrier:
//l.handleReadBarrier(req)
//case <-timer.C:
//l.broadcastHeartbeat()
//case <-l.syncer.ctrl.Closed():
//l.logger.Error("Sync'er closed: %v", l.syncer.ctrl.Failure())
//becomeFollower(l.replica)
//return
//}

//l.logger.Debug("Resetting timeout [%v]", l.replica.ElectionTimeout/5)
//timer.Reset(l.replica.ElectionTimeout / 5)
//}
//}()

//// Establish leadership
//if !l.broadcastHeartbeat() {
//becomeFollower(l.replica)
//l.ctrl.Close()
//return
//}

//// Establish read barrier
//if _, err := l.replica.LocalAppend(appendEvent{Event{}, NoOp}); err != nil {
//becomeFollower(l.replica)
//l.ctrl.Close()
//return
//}
//}

//// leaders do not accept snapshot installations
//func (c *leader) handleInstallSnapshot(req *context.Request) {
//snapshot := req.Body().(installSnapshot)
//if snapshot.term <= c.term.Num {
//req.Ack(newResponse(c.term.Num, false))
//return
//}

//c.replica.Term(snapshot.term, &snapshot.leaderId, &snapshot.leaderId)
//req.Ack(newResponse(snapshot.term, false))
//becomeFollower(c.replica)
//c.ctrl.Close()
//}

//// leaders do not accept replication requests
//func (c *leader) handleReplication(req *context.Request) {
//append := req.Body().(replicate)
//if append.term <= c.term.Num {
//req.Ack(newResponse(c.term.Num, false))
//return
//}

//c.replica.Term(append.term, &append.id, &append.id)
//req.Ack(newResponse(append.term, false))
//becomeFollower(c.replica)
//c.ctrl.Close()
//}

//func (c *leader) handleReadBarrier(req *context.Request) {
//if c.broadcastHeartbeat() {
//req.Ack(c.replica.Log.Committed())
//return
//}

//req.Fail(NotLeaderError)
//becomeFollower(c.replica)
//c.ctrl.Close()
//}

//func (c *leader) handleRemoteAppend(req *context.Request) {
//err := c.proxyPool.SubmitOrCancel(req.Canceled(), func() {
//req.Return(c.replica.LocalAppend(req.Body().(appendEvent)))
//})

//if err != nil {
//req.Fail(errors.Wrapf(err, "Error submitting work to proxy pool."))
//}
//}

//func (c *leader) handleLocalAppend(req *context.Request) {
//err := c.appendPool.SubmitOrCancel(req.Canceled(), func() {
//req.Return(c.syncer.Append(req.Body().(appendEvent)))
//c.broadcastHeartbeat()
//})

//if err != nil {
//req.Fail(errors.Wrapf(err, "Error submitting work to append pool."))
//}
//}

//func (c *leader) handleRosterUpdate(req *context.Request) {
//update := req.Body().(rosterUpdate)

//all := c.replica.Cluster()
//if !update.join {
//all = delPeer(all, update.peer)

//c.logger.Info("Removing peer: %v", update.peer)
//if _, e := c.replica.Append(clusterBytes(all), Conf); e != nil {
//req.Fail(e)
//return
//} else {
//req.Ack(true)
//return
//}
//}

//var err error
//defer func() {
//if err != nil {
//c.logger.Info("Error adding peer [%v].  Removing from sync'ers.", update.peer)
//c.syncer.handleRosterChange(delPeer(all, update.peer))
//}
//}()

//all = addPeer(all, update.peer)
//c.syncer.handleRosterChange(all)
//sync := c.syncer.Syncer(update.peer.Id)

//_, err = sync.heartbeat(req.Canceled())
//if err != nil {
//req.Fail(err)
//return
//}

//score, err := sync.score(req.Canceled())
//if err != nil {
//req.Fail(err)
//becomeFollower(c.replica)
//c.ctrl.Fail(err)
//return
//}

//if score < 0 {
//req.Fail(errors.Wrapf(context.TimeoutError, "Unable to merge peer: %v", update.peer))
//return
//}

//c.logger.Info("Joining peer [%v] with peer [%v]", update.peer, score)
//if _, e := c.replica.Append(clusterBytes(all), Conf); e != nil {
//req.Fail(e)
//return
//} else {
//req.Ack(true)
//return
//}
//}

//func (c *leader) handleRequestVote(req *context.Request) {
//vote := req.Body().(requestVote)
//c.logger.Debug("Handling request vote: %v", vote)

//// handle: previous or current term vote.  (immediately decline.  already leader)
//if vote.term <= c.term.Num {
//req.Ack(newResponse(c.term.Num, false))
//return
//}

//// handle: future term vote.  (move to new term.  only accept if candidate log is long enough)
//maxIndex, maxTerm, err := c.replica.Log.Last()
//if err != nil {
//req.Ack(newResponse(vote.term, false))
//return
//}

//if vote.maxLogIndex >= maxIndex && vote.maxLogTerm >= maxTerm {
//c.replica.Term(vote.term, nil, &vote.id)
//req.Ack(newResponse(vote.term, true))
//} else {
//c.replica.Term(vote.term, nil, nil)
//req.Ack(newResponse(vote.term, false))
//}

//becomeFollower(c.replica)
//c.ctrl.Close()
//}

//func (c *leader) broadcastHeartbeat() bool {
//syncers := c.syncer.Syncers()
//ch := make(chan response)
//for _, p := range syncers {
//go func(p *peerSyncer) {
//resp, err := p.heartbeat(c.ctrl.Closed())
//if err != nil {
//ch <- newResponse(c.term.Num, false)
//} else {
//ch <- resp
//}
//}(p)
//}

//timer := c.ctx.Timer(c.replica.ElectionTimeout)
//defer timer.Close()
//for i := 0; i < c.replica.Majority()-1; {
//select {
//case <-c.ctrl.Closed():
//return false
//case resp := <-ch:
//if resp.term > c.term.Num {
//c.replica.Term(resp.term, nil, c.term.VotedFor)
//becomeFollower(c.replica)
//c.ctrl.Close()
//return false
//}

//i++
//case <-timer.Closed():
//c.logger.Error("Unable to retrieve enough heartbeat responses.")
//c.replica.Term(c.term.Num, nil, c.term.VotedFor)
//becomeFollower(c.replica)
//c.ctrl.Close()
//return false
//}
//}

//return true
//}
