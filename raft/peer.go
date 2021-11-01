package raft

import (
	"fmt"
	"time"

	"github.com/pkopriv2/golang-sdk/lang/net"
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

func (p Peer) ClientPool(network net.Network, timeout time.Duration, num int) net.ConnectionPool {
	return net.NewConnectionPool(num, timeout, func(timeout time.Duration) (net.Connection, error) {
		return network.Dial(timeout, p.Addr)
	})
}

//func (p Peer) Client(ctx common.Context, network net.Network, timeout time.Duration) (*rpcClient, error) {
//raw, err := network.Dial(timeout, p.Addr)
//if raw == nil || err != nil {
//return nil, errors.Wrapf(err, "Error connecting to peer [%v]", p)
//}

//cl, err := net.NewClient(ctx, raw, net.Json)
//if cl == nil || err != nil {
//return nil, err
//}

//return &rpcClient{cl}, nil
//}
