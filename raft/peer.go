package raft

import (
	"fmt"
	"io"

	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/pool"
	uuid "github.com/satori/go.uuid"
)

// This contains the primary identifier for a peer.
type Peer struct {
	Id   uuid.UUID `json:"id"`
	Addr string    `json:"addr"`
}

func newPeer(addr string) Peer {
	return Peer{Id: uuid.NewV1(), Addr: addr}
}

func (p Peer) ClientPool(ctrl context.Control, opts Options) pool.ObjectPool {
	return pool.NewObjectPool(ctrl, opts.MaxConnsPerPeer, func() (io.Closer, error) {
		return p.Dial(opts)
	})
}

func (p Peer) Dial(opts Options) (*Client, error) {
	return Dial(opts.Transport, p.Addr, opts.Timeouts())
}

func (p Peer) String() string {
	return fmt.Sprintf("Peer(%v, %v)", p.Id.String()[:8], p.Addr)
}

// replicated configuration
type Peers map[uuid.UUID]Peer

func NewPeers(peers []Peer) (ret Peers) {
	ret = make(map[uuid.UUID]Peer)
	for _, p := range peers {
		ret[p.Id] = p
	}
	return
}

func (peers Peers) Copy() (ret Peers) {
	ret = make(map[uuid.UUID]Peer)
	for _, p := range peers {
		ret[p.Id] = p
	}
	return
}

func (peers Peers) Flatten() (ret []Peer) {
	ret = make([]Peer, 0, len(peers))
	for _, p := range peers {
		ret = append(ret, p)
	}
	return
}

func (peers Peers) Contains(p Peer) (ok bool) {
	_, ok = peers[p.Id]
	return
}

func (peers Peers) Add(p Peer) Peers {
	if peers.Contains(p) {
		return peers
	}
	return NewPeers(append(peers.Flatten(), p))
}

func (peers Peers) Delete(p Peer) Peers {
	all := peers.Copy()
	delete(all, p.Id)
	return all
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
