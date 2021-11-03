package raft

import (
	"fmt"
	"io"

	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/pool"
	uuid "github.com/satori/go.uuid"
)

// A peer contains the identifying info of a cluster member.
type Peer struct {
	Id   uuid.UUID `json:"id"`
	Addr string    `json:"addr"`
}

func newPeer(addr string) Peer {
	return Peer{Id: uuid.NewV1(), Addr: addr}
}

func (p Peer) String() string {
	return fmt.Sprintf("Peer(%v, %v)", p.Id.String()[:8], p.Addr)
}

// replicated configuration
type Peers []Peer

func (peers Peers) Contains(p Peer) (ok bool) {
	for _, cur := range peers {
		if cur.Id == p.Id {
			ok = true
			return
		}
	}
	return
}

func SearchPeersById(id uuid.UUID) func(Peer) bool {
	return func(p Peer) bool {
		return p.Id == id
	}
}

func (peers Peers) First(fn func(p Peer) bool) *Peer {
	for _, cur := range peers {
		if fn(cur) {
			return &cur
		}
	}
	return nil
}

func (peers Peers) Find(p Peer) int {
	for i, cur := range peers {
		if cur.Id == p.Id {
			return i
		}
	}
	return -1
}

func (peers Peers) Add(p Peer) Peers {
	if peers.Contains(p) {
		return peers
	}
	return append(peers, p)
}

func (peers Peers) Delete(p Peer) Peers {
	index := peers.Find(p)
	if index == -1 {
		return peers
	}
	return append(peers[:index], peers[index+1:]...)
}

func (peers Peers) Equals(o Peers) bool {
	if len(peers) != len(o) {
		return false
	}

	for _, p := range peers {
		if !o.Contains(p) {
			return false
		}
	}
	return true
}

func (p Peer) ClientPool(ctrl context.Control, opts Options) pool.ObjectPool {
	return pool.NewObjectPool(ctrl, opts.MaxConns, func() (io.Closer, error) {
		return p.Dial(opts)
	})
}

func (p Peer) Dial(opts Options) (*rpcClient, error) {
	return dialRpcClient(p.Addr, opts)
}
