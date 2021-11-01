package raft

//type logClient struct {
//id     uuid.UUID
//ctx    common.Context
//ctrl   common.Control
//logger common.Logger
//pool   common.ObjectPool // T: *rpcClient
//self   *replica
//}

//func newLogClient(self *replica, pool common.ObjectPool) *logClient {
//ctx := self.Ctx.Sub("LogClient")
//return &logClient{
//id:     self.Id,
//ctx:    ctx,
//logger: ctx.Logger(),
//ctrl:   ctx.Control(),
//self:   self,
//pool:   pool,
//}
//}

//func (s *logClient) Id() uuid.UUID {
//return s.id
//}

//func (c *logClient) Close() error {
//return c.ctrl.Close()
//}

//func (s *logClient) Head() int {
//return s.self.Log.Head()
//}

//func (s *logClient) Committed() int {
//return s.self.Log.Committed()
//}

//func (s *logClient) Compact(until int, data <-chan Event, size int) error {
//return s.self.Compact(until, data, size)
//}

//func (s *logClient) Listen(start int, buf int) (Listener, error) {
//raw, err := s.self.Log.ListenCommits(start, buf)
//if err != nil {
//return nil, err
//}
//return newLogClientListener(raw), nil
//}

//func (c *logClient) Append(cancel <-chan struct{}, e Event) (Entry, error) {
//return c.append(cancel, e, Std)
//}

//func (s *logClient) Snapshot() (int, EventStream, error) {
//snapshot, err := s.self.Log.Snapshot()
//if err != nil {
//return 0, nil, err
//}

//return snapshot.LastIndex(), newSnapshotStream(s.ctrl, snapshot, 1024), nil
//}

//func (c *logClient) append(cancel <-chan struct{}, e Event, k Kind) (entry Entry, err error) {
//raw := c.pool.TakeOrCancel(cancel)
//if raw == nil {
//return Entry{}, errors.WithStack(common.CanceledError)
//}
//defer func() {
//if err != nil {
//c.pool.Fail(raw)
//} else {
//c.pool.Return(raw)
//}
//}()
//resp, err := raw.(*rpcClient).Append(appendEvent{e, k})
//if err != nil {
//return Entry{}, err
//}

//return Entry{resp.index, e, resp.term, Std}, nil
//}

//type logClientListener struct {
//raw Listener
//dat chan Entry
//}

//func newLogClientListener(raw Listener) *logClientListener {
//l := &logClientListener{raw, make(chan Entry)}
//l.start()
//return l
//}

//func (p *logClientListener) start() {
//go func() {
//for {
//var e Entry
//select {
//case <-p.raw.Ctrl().Closed():
//return
//case e = <-p.raw.Data():
//}

//if e.Kind != Std {
//e = Entry{e.Index, []byte{}, e.Term, NoOp}
//}

//select {
//case <-p.raw.Ctrl().Closed():
//return
//case p.dat <- e:
//}
//}
//}()
//}

//func (l *logClientListener) Close() error {
//return l.raw.Close()
//}

//func (l *logClientListener) Ctrl() common.Control {
//return l.raw.Ctrl()
//}

//func (l *logClientListener) Data() <-chan Entry {
//return l.dat
//}
