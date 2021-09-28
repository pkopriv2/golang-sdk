package dag

import (
	"errors"
	"sync"
)

// Traverse options are used to control behavior of the visitor logic.
type TraverseOptions struct {
	Cancel      <-chan struct{}
	Parallelism int
}

func WithCanceler(c <-chan struct{}) func(*TraverseOptions) {
	return func(w *TraverseOptions) {
		w.Cancel = c
	}
}

func WithParallelism(n int) func(*TraverseOptions) {
	return func(w *TraverseOptions) {
		w.Parallelism = n
	}
}

func buildTraverseOptions(fns []func(*TraverseOptions)) (ret TraverseOptions) {
	ret = TraverseOptions{Parallelism: 1}
	for _, fn := range fns {
		fn(&ret)
	}
	return
}

// A traverse function is applied to vertices as they are visited.
type TraverseFunc func(<-chan struct{}, Vertex) error

// The result of visiting a vertex.
type Result struct {
	Vertex
	Error error
}

// A snapshot contains the state of a traverser.  This is an immutable
// data structure and is safe for concurrent use.
type Snapshot struct {
	pending map[string]Vertex
	running map[string]Vertex
	results map[string]Result
	failure error
}

func EmptySnapshot() (ret *Snapshot) {
	return &Snapshot{
		pending: make(map[string]Vertex),
		running: make(map[string]Vertex),
		results: make(map[string]Result)}
}

func (s *Snapshot) Pending() (ret []Vertex) {
	for _, v := range s.pending {
		ret = append(ret, v)
	}
	return
}

func (s *Snapshot) Running() (ret []Vertex) {
	for _, v := range s.running {
		ret = append(ret, v)
	}
	return
}

func (s *Snapshot) Results() (ret map[string]Result) {
	ret = copyResults(s.results)
	return
}

func (s *Snapshot) Error() (err error) {
	if s.failure != nil {
		err = s.failure
		return
	}

	for _, r := range s.results {
		if r.Error != nil {
			err = r.Error
			return
		}
	}
	return
}

func (s *Snapshot) AnyPending() (ret Vertex) {
	for _, v := range s.pending {
		ret = v
		return
	}
	return
}

func (s *Snapshot) AddPending(all ...Vertex) *Snapshot {
	pending := copyVertices(s.pending)
	for _, v := range all {
		pending[v.Id] = v
	}
	return &Snapshot{pending, s.running, s.results, s.failure}
}

func (s *Snapshot) AddRunning(all ...Vertex) *Snapshot {
	pending := copyVertices(s.pending)
	running := copyVertices(s.running)
	for _, v := range all {
		delete(pending, v.Id)
		running[v.Id] = v
	}
	return &Snapshot{pending, running, s.results, s.failure}
}

func (s *Snapshot) AddResults(res ...Result) *Snapshot {
	running := copyVertices(s.running)
	results := copyResults(s.results)
	for _, r := range res {
		delete(running, r.Id)
		results[r.Id] = r
	}
	return &Snapshot{s.pending, running, results, s.failure}
}

func (s *Snapshot) SetFailure(err error) *Snapshot {
	return &Snapshot{s.pending, s.running, s.results, err}
}

type Traverser struct {
	graph *Graph
	fn    TraverseFunc
	opts  TraverseOptions

	lock     *sync.RWMutex
	snapshot *Snapshot

	wait    *sync.WaitGroup
	done    chan Result
	closer  chan struct{}
	closing chan struct{}
	closed  chan struct{}
}

func newTraverser(g *Graph, fn TraverseFunc, o ...func(*TraverseOptions)) (ret *Traverser, err error) {
	opts := buildTraverseOptions(o)
	if opts.Parallelism < 1 {
		err = errors.New("Parallelism must be positive")
		return
	}

	ret = &Traverser{
		graph:    g,
		fn:       fn,
		opts:     opts,
		lock:     &sync.RWMutex{},
		snapshot: EmptySnapshot(),
		wait:     &sync.WaitGroup{},
		done:     make(chan Result),
		closer:   make(chan struct{}, 1),
		closing:  make(chan struct{}),
		closed:   make(chan struct{}),
	}
	return
}

func (t *Traverser) Snaphost() *Snapshot {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.snapshot
}

func (t *Traverser) setSnapshot(s *Snapshot) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.snapshot = s
}

func (t *Traverser) fail(err error) {
	select {
	case <-t.closed:
		return
	case t.closer <- struct{}{}:
	}
	close(t.closing)
	t.wait.Wait()
	t.setSnapshot(t.Snaphost().SetFailure(err))
	close(t.closed)
}

func (t *Traverser) Close() error {
	t.fail(errors.New("Traverser:Closed"))
	return t.Wait()
}

func (t *Traverser) Closed() <-chan struct{} {
	return t.closed
}

func (t *Traverser) Wait() error {
	<-t.Closed()
	return t.Snaphost().Error()
}

func (t *Traverser) Stop() {
	t.fail(errors.New("Traverser:Stopped"))
}

func (t *Traverser) Start() {
	go func() {
		s := t.Snaphost().AddPending(t.graph.Entry()...)
		for {
			for len(s.pending) > 0 && len(s.running) < t.opts.Parallelism {
				current := s.AnyPending()

				t.wait.Add(1)
				go func(v Vertex) {
					defer t.wait.Done()
					select {
					case <-t.closing:
					case t.done <- Result{v, t.fn(t.closing, v)}:
					}
				}(current)

				s = s.AddRunning(current)
			}

			if len(s.running) == 0 {
				t.fail(nil)
				return
			}

			t.setSnapshot(s)
			var result Result
			select {
			case <-t.opts.Cancel:
				t.fail(errors.New("Traverser:Canceled"))
				return
			case <-t.closing:
				return
			case result = <-t.done:
				s = s.AddResults(result)
				t.setSnapshot(s)
			}

			// If an error occurs, we cannot continue downstream
			if result.Error != nil {
				continue
			}

		Outer:
			// Evaluate all downstream neighbors. Add any whose dependencies
			// have all been visited to the pending list.
			for _, v := range t.graph.DownstreamNeighbors(result.Vertex) {
				for _, u := range t.graph.UpstreamNeighbors(v) {
					if result, ok := s.results[u.Id]; !ok || result.Error != nil {
						continue Outer
					}
				}
				s = s.AddPending(v)
			}

			t.setSnapshot(s)
		}
	}()
}

func copyVertices(orig map[string]Vertex) (copy map[string]Vertex) {
	copy = make(map[string]Vertex)
	for k, v := range orig {
		copy[k] = v
	}
	return
}

func copyResults(orig map[string]Result) (copy map[string]Result) {
	copy = make(map[string]Result)
	for k, v := range orig {
		copy[k] = v
	}
	return
}
