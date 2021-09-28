package dag

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_Incomplete_EdgeWNoSrc(t *testing.T) {
	_, err := NewBuilder().
		AddVertex("a", nil).
		AddEdge("b", "a").
		Build()
	assert.NotNil(t, err)
	assert.IsType(t, &InvalidEdgeError{}, err)
}

func TestBuilder_Incomplete_EdgeWNoDst(t *testing.T) {
	_, err := NewBuilder().
		AddVertex("a", nil).
		AddEdge("a", "b").
		Build()
	assert.NotNil(t, err)
	assert.IsType(t, &InvalidEdgeError{}, err)
}

func TestBuilder_Invalid_EdgeWSelfReference(t *testing.T) {
	_, err := NewBuilder().
		AddVertex("a", nil).
		AddEdge("a", "a").
		Build()
	assert.NotNil(t, err)
	assert.IsType(t, &InvalidEdgeError{}, err)
}

func TestBuilder_SingleCycle_TwoNodes(t *testing.T) {
	_, err := NewBuilder().
		AddVertex("a", nil).
		AddVertex("b", nil).
		AddEdge("a", "b").
		AddEdge("b", "a").
		Build()
	assert.NotNil(t, err)
	assert.IsType(t, &CycleError{}, err)
}

func TestBuilder_SingleCycle_ThreeNodes(t *testing.T) {
	_, err := NewBuilder().
		AddVertex("a", nil).
		AddVertex("b", nil).
		AddVertex("c", nil).
		AddEdge("a", "b").
		AddEdge("b", "c").
		AddEdge("c", "a").
		Build()
	assert.NotNil(t, err)
	assert.IsType(t, &CycleError{}, err)
}

func TestBuilder_MultiCycle(t *testing.T) {
	_, err := NewBuilder().
		AddVertex("a", nil).
		AddVertex("b", nil).
		AddVertex("c", nil).
		AddVertex("d", nil).
		AddVertex("e", nil).
		AddEdge("a", "b").
		AddEdge("b", "c").
		AddEdge("c", "a").
		AddEdge("d", "e").
		AddEdge("e", "d").
		Build()
	assert.NotNil(t, err)
	assert.IsType(t, &CycleError{}, err)
}

func TestBuilder_Empty(t *testing.T) {
	g, err := NewBuilder().
		Build()
	if !assert.Nil(t, err) {
		return
	}
	assert.True(t, g.IsEmpty())
}

func TestBuilder_SingleVertex(t *testing.T) {
	g, err := NewBuilder().
		AddVertex("a", nil).
		Build()
	if !assert.Nil(t, err) {
		return
	}
	assert.False(t, g.IsEmpty())
}

func TestBuilder_MultiVertex(t *testing.T) {
	g, err := NewBuilder().
		AddVertex("a", nil).
		AddVertex("b", nil).
		Build()
	if !assert.Nil(t, err) {
		return
	}
	assert.False(t, g.IsEmpty())
}

func TestBuilder_MultiVertex_SingleEdge(t *testing.T) {
	g, err := NewBuilder().
		AddVertex("a", nil).
		AddVertex("b", nil).
		AddEdge("a", "b").
		Build()
	if !assert.Nil(t, err) {
		return
	}
	assert.False(t, g.IsEmpty())
}

func TestIsTopoSort_EmptyGraph_EmptyOrder(t *testing.T) {
	g := NewBuilder().MustBuild()
	assert.True(t, g.IsTopologicalSort([]Vertex{}))
}

func TestIsTopoSort_EmptyGraph_NonEmptyOrder(t *testing.T) {
	g := NewBuilder().MustBuild()
	assert.False(t, g.IsTopologicalSort([]Vertex{Vertex{Id: "a"}}))
}

func TestIsTopoSort_NonEmptyGraph_EmptyOrder(t *testing.T) {
	g := NewBuilder().AddVertex("a", nil).MustBuild()
	assert.False(t, g.IsTopologicalSort([]Vertex{}))
}

func TestIsTopoSort_EqualSize_BadOrder(t *testing.T) {
	g := NewBuilder().
		AddVertex("a", nil).
		AddVertex("b", nil).
		AddEdge("b", "a").
		MustBuild()
	assert.False(t, g.IsTopologicalSort([]Vertex{Vertex{Id: "a"}, Vertex{Id: "b"}}))
}

func TestIsTopoSort_MultipleOrderings(t *testing.T) {
	g := NewBuilder().
		AddVertex("a", nil).
		AddVertex("b", nil).
		AddVertex("c", nil).
		AddEdge("a", "b").
		MustBuild()
	assert.True(t, g.IsTopologicalSort([]Vertex{Vertex{Id: "c"}, Vertex{Id: "a"}, Vertex{Id: "b"}}))
	assert.True(t, g.IsTopologicalSort([]Vertex{Vertex{Id: "a"}, Vertex{Id: "b"}, Vertex{Id: "c"}}))
	assert.True(t, g.IsTopologicalSort([]Vertex{Vertex{Id: "a"}, Vertex{Id: "c"}, Vertex{Id: "b"}}))
	assert.False(t, g.IsTopologicalSort([]Vertex{Vertex{Id: "c"}, Vertex{Id: "b"}, Vertex{Id: "a"}}))
}

func TestWalk(t *testing.T) {
	g, err := NewBuilder().
		AddVertex("a", nil).
		AddVertex("b", nil).
		AddVertex("c", nil).
		AddVertex("d", nil).
		AddVertex("e", nil).
		AddEdge("a", "b").
		AddEdge("a", "c").
		AddEdge("d", "e").
		Build()
	if !assert.Nil(t, err) {
		return
	}

	lock := &sync.Mutex{}
	var order []Vertex
	tr, err := g.StartTraverse(func(c <-chan struct{}, v Vertex) (err error) {
		lock.Lock()
		defer lock.Unlock()

		order = append(order, v)
		return
	}, WithParallelism(2))
	if !assert.Nil(t, err) {
		return
	}
	if !assert.Nil(t, tr.Wait()) {
		return
	}

	assert.True(t, g.IsTopologicalSort(order))
}

func TestWalk_SingleChain(t *testing.T) {
	builder := NewBuilder()

	for i := 0; i < 100; i++ {
		builder = builder.AddVertex(fmt.Sprint(i), nil)
		if i > 0 {
			builder = builder.AddEdge(fmt.Sprint(i-1), fmt.Sprint(i))
		}
	}

	g, err := builder.Build()
	if !assert.Nil(t, err) {
		return
	}

	lock := &sync.Mutex{}
	var order []Vertex
	err = g.Traverse(func(c <-chan struct{}, v Vertex) (err error) {
		lock.Lock()
		defer lock.Unlock()
		order = append(order, v)
		return
	}, WithParallelism(10))
	if !assert.Nil(t, err) {
		return
	}
	assert.True(t, g.IsTopologicalSort(order))
}
