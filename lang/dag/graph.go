package dag

import (
	"fmt"

	"github.com/pkopriv2/golang-sdk/lang/concurrent"
)

// Simple rename.  Typical entry-point
func NewGraph() *Builder {
	return NewBuilder()
}

// A cycle error contains all the vertices that were discovered
// to be involved in a cycle within a directed graph.
type CycleError struct {
	Cycles [][]Vertex
}

func (c *CycleError) Error() string {
	return fmt.Sprintf("Cycles detected %v", c.Cycles)
}

// An invalid edge error is produced when an edge references a
// non-existent vertex or is self-referencing.
type InvalidEdgeError struct {
	Edge Edge
	Msg  string
}

func (c *InvalidEdgeError) Error() string {
	return fmt.Sprintf("Edge [%v] is invalid [%v]", c.Edge, c.Msg)
}

// A directed edge.
type Edge struct {
	Src string
	Dst string
}

func (e Edge) String() string {
	return fmt.Sprintf("Edge(%v->%v)", e.Src, e.Dst)
}

// A vertex is a node within a directed graph
type Vertex struct {
	Id   string
	Data interface{}
}

func (v Vertex) String() string {
	return fmt.Sprintf("Vertex(%v)", v.Id)
}

// A simple set-like collection of edges.
type Edges []Edge

func (e Edges) Contains(src, dst string) bool {
	for _, cur := range e {
		if src == cur.Src && dst == cur.Dst {
			return true
		}
	}
	return false
}

func (e Edges) Equals(o Edges) bool {
	if len(e) != len(o) {
		return false
	}

	for _, cur := range o {
		if !e.Contains(cur.Src, cur.Dst) {
			return false
		}
	}
	return true
}

func (e Edges) Add(src, dst string) Edges {
	if e.Contains(src, dst) {
		return e
	}
	return append(e, Edge{src, dst})
}

// A simple set-like collection of vertices
type Vertices []Vertex

func (v Vertices) Contains(id string) bool {
	for _, cur := range v {
		if id == cur.Id {
			return true
		}
	}
	return false
}

func (v Vertices) Equals(o Vertices) bool {
	if len(v) != len(o) {
		return false
	}
	for _, cur := range o {
		if !v.Contains(cur.Id) {
			return false
		}
	}
	return true
}

func (v Vertices) Add(id string, data interface{}) (ret Vertices) {
	for _, cur := range v {
		if cur.Id != id {
			ret = append(ret, cur)
		}
	}
	return append(ret, Vertex{id, data})
}

// A builder is responsible for safely building new graph instances.
type Builder struct {
	edges    Edges
	vertices Vertices
}

// Returns a new graph builder.
func NewBuilder() *Builder {
	return &Builder{}
}

// Adds a vertex to the builder, returning a new builder.  The
// original builder is NOT mutated and is safe to reuse.
func (b *Builder) AddVertex(id string, data interface{}) (ret *Builder) {
	return &Builder{b.edges, b.vertices.Add(id, data)}
}

// Adds an edge to the builder, returning a new builder.  The
// original builder is NOT mutated and is safe to reuse.
func (b *Builder) AddEdge(src, dst string) *Builder {
	return &Builder{b.edges.Add(src, dst), b.vertices}
}

// Builds the resulting graph.  Ensures that the graph is both
// complete and cycle free.
func (b *Builder) Build() (ret *Graph, err error) {
	cycles, err := detectCycles(b.vertices, b.edges)
	if err != nil {
		return
	}

	if len(cycles) > 0 {
		err = &CycleError{cycles}
		return
	}

	ret = &Graph{
		indexVertices(b.vertices),
		indexEdgesBySrc(b.edges),
		indexEdgesByDst(b.edges)}
	return
}

// Builds the resulting graph.  Panics if the graph contains errors.
func (b *Builder) MustBuild() (ret *Graph) {
	ret, err := b.Build()
	if err != nil {
		panic(err)
	}
	return
}

// A graph is an implementation of of a directed acyclic graph
type Graph struct {
	vertices   map[string]Vertex
	edgesBySrc map[string][]Edge // indexed by src vertex
	edgesByDst map[string][]Edge // indexed by dst vertex
}

// Returns whether the graph is empty
func (g *Graph) IsEmpty() bool {
	return len(g.vertices) == 0
}

// Returns whether the graph is empty
func (g *Graph) ContainsVertex(id string) (ok bool) {
	_, ok = g.vertices[id]
	return
}

// Returns the complete set of vertices in the graph
func (g *Graph) Vertices() Vertices {
	return flattenVertices(g.vertices)
}

// Returns the complete set of edges in the graph
func (g *Graph) Edges() Edges {
	return flattenEdges(g.edgesBySrc)
}

// Returns whether the input graph is equal to this graph
func (g *Graph) Equals(o *Graph) (ok bool) {
	if o == nil {
		return g == nil
	}

	return g.Edges().Equals(o.Edges()) && g.Vertices().Equals(o.Vertices())
}

// Returns a new builder whose set of edges and vertices are
// equal to this graph.
func (g *Graph) Update() *Builder {
	return &Builder{flattenEdges(g.edgesBySrc), flattenVertices(g.vertices)}
}

// Returns all the upstream vertices immediately adjacent to v.  Returns nil if the vertex is not a member
func (g *Graph) UpstreamNeighbors(id string) (ret []Vertex) {
	for _, e := range g.edgesByDst[id] {
		ret = append(ret, g.vertices[e.Src])
	}
	return
}

// Returns all the upstream vertices immediately adjacent to v.  Returns nil if the vertex is not a member
func (g *Graph) DownstreamNeighbors(id string) (ret []Vertex) {
	for _, e := range g.edgesBySrc[id] {
		ret = append(ret, g.vertices[e.Dst])
	}
	return
}

// Returns a graph that is reachable from the input vertex.  Returns nil if the vertex is not a member
func (g *Graph) DownstreamGraph(id string, inclusive bool) (ret *Graph) {
	if !g.ContainsVertex(id) {
		return nil
	}

	builder := NewBuilder()

	stack := concurrent.NewArrayStack() // only convenient stack implementation.  replace later. (Not even sure if a stack is necessary)
	stack.Push(g.vertices[id])

	var cur interface{} // will type it later
	for {
		cur = stack.Pop()
		if cur == nil {
			break
		}

		curVertex := cur.(Vertex)
		if curVertex.Id != id || inclusive {
			builder = builder.AddVertex(curVertex.Id, curVertex.Data)
		}

		for _, e := range g.edgesBySrc[curVertex.Id] {
			if curVertex.Id != id || inclusive {
				builder = builder.AddEdge(e.Src, e.Dst)
			}

			// not possible for there not to be a target vertex
			stack.Push(g.vertices[e.Dst])
		}
	}

	ret, _ = builder.Build()
	return
}

// Returns all the entry vertices.  These are the vertices
// that have no upstream dependencies.
func (g *Graph) Entry() (ret []Vertex) {
	if len(g.vertices) == 0 {
		return
	}

	for _, v := range g.vertices {
		if _, ok := g.edgesByDst[v.Id]; !ok {
			ret = append(ret, v)
		}
	}
	return
}

// Returns whether the input set of vertices is a topological
// ordering of vertices contained within the graph.
func (g *Graph) IsTopologicalSort(in []Vertex) (ok bool) {
	if len(g.vertices) != len(in) {
		return
	}

	findIndex := func(id string) int {
		for i, u := range in {
			if u.Id == id {
				return i
			}
		}
		return -1
	}

	for _, e := range g.Edges() {
		srcIdx, dstIdx := findIndex(e.Src), findIndex(e.Dst)
		if srcIdx == -1 || dstIdx == -1 || srcIdx >= dstIdx {
			return
		}
	}

	ok = true
	return
}

// Walks the graph in a manner that guarantees a proper topological ordering.
func (g *Graph) StartTraverse(fn TraverseFunc, o ...func(*TraverseOptions)) (ret *Traverser, err error) {
	ret, err = newTraverser(g, fn, o...)
	if err != nil {
		return
	}
	ret.Start()
	return
}

// Walks the graph in a manner that guarantees a proper topological ordering.
func (g *Graph) Traverse(fn TraverseFunc, o ...func(*TraverseOptions)) (err error) {
	t, err := g.StartTraverse(fn, o...)
	if err != nil {
		return
	}
	err = t.Wait()
	return
}

func flattenEdges(in map[string][]Edge) (out Edges) {
	for _, set := range in {
		for _, e := range set {
			out = out.Add(e.Src, e.Dst)
		}
	}
	return
}

func flattenVertices(in map[string]Vertex) (out []Vertex) {
	for _, v := range in {
		out = append(out, v)
	}
	return
}

func indexVertices(in []Vertex) (out map[string]Vertex) {
	out = make(map[string]Vertex)
	for _, v := range in {
		out[v.Id] = v
	}
	return
}

func indexEdgesBySrc(in []Edge) (out map[string][]Edge) {
	out = make(map[string][]Edge)
	for _, e := range in {
		out[e.Src] = append(out[e.Src], e)
	}
	return
}

func indexEdgesByDst(in []Edge) (out map[string][]Edge) {
	out = make(map[string][]Edge)
	for _, e := range in {
		out[e.Dst] = append(out[e.Dst], e)
	}
	return
}

func detectCycles(v []Vertex, e []Edge) (ret [][]Vertex, err error) {

	// Build the necessary indexed versions of the inputs
	vertices, edgesBySrc, edgesByDst :=
		indexVertices(v),
		indexEdgesBySrc(e),
		indexEdgesByDst(e)

	// Ensure that the graph is complete
	for _, edge := range e {
		if edge.Src == edge.Dst {
			err = &InvalidEdgeError{edge, "References self"}
			return
		}
		if _, ok := vertices[edge.Src]; !ok {
			err = &InvalidEdgeError{edge, fmt.Sprintf("Missing source vertex [%v]", edge.Src)}
			return
		}
		if _, ok := vertices[edge.Dst]; !ok {
			err = &InvalidEdgeError{edge, fmt.Sprintf("Missing target vertex [%v]", edge.Src)}
			return
		}
	}

	// Retrieves the vertices immediately downstream from u
	outNeighbors := func(u Vertex) (ret []Vertex) {
		for _, e := range edgesBySrc[u.Id] {
			ret = append(ret, vertices[e.Dst])
		}
		return
	}

	// Retrieves the vertices immediately upstream from u
	inNeighbors := func(u Vertex) (ret []Vertex) {
		for _, e := range edgesByDst[u.Id] {
			ret = append(ret, vertices[e.Src])
		}
		return
	}

	// We'll keep track of two indexes.  The first keeps track
	// of which nodes have been visited (preventing duplication).
	// The second will keep track of which subcomponent each vertex
	// belongs to.
	visited, assigned :=
		make(map[string]bool),
		make(map[string]string)

	sorted := make([]Vertex, 0, len(v))

	// Visits a node and all the nodes accessible via it.  This has
	// the side effect of adding the vertex so that it comes BEFORE
	// all its downstream neighbors.
	var visit func(Vertex)
	visit = func(u Vertex) {
		if visited[u.Id] {
			return
		}

		visited[u.Id] = true
		for _, v := range outNeighbors(u) {
			visit(v)
		}

		sorted = append([]Vertex{u}, sorted...)
	}

	// Assigns a vertex to a subcomponent
	var assign func(Vertex, Vertex)
	assign = func(u, root Vertex) {
		if _, ok := assigned[u.Id]; ok {
			return
		}

		assigned[u.Id] = root.Id
		for _, v := range inNeighbors(u) {
			assign(v, root)
		}
	}

	for _, u := range v {
		visit(u)
	}

	for _, v := range sorted {
		assign(v, v)
	}

	componentsByRoot := make(map[string][]Vertex)
	for vId, rootId := range assigned {
		componentsByRoot[rootId] = append(componentsByRoot[rootId], vertices[vId])
	}

	for _, component := range componentsByRoot {
		if len(component) > 1 {
			ret = append(ret, component)
		}
	}
	return
}
