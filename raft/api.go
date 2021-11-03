// Raft allows consumers to utilize a generic, replicated event
// log in order to create highly-resilient, strongly consistent
// replicated state machines.
//
// # Distributed Consensus
//
// Underpinning every state machine is a log that implements the Raft
// distributed consensus protocol [1].  While most of the details of the
// protocol are abstracted, it is useful to know some of the high level
// details.
//
// Before any progress can be made in a raft cluster, the members
// of the cluster must elect a leader.  Moreover, most interactions
// require a leader.  Raft uses an election system - based on majority
// decision - to elect a leader.  Once established, the leader
// maintains its position through the use of heartbeats.
//
// If a leader dies or becomes unreachable, the previous followers
// will hold an election to determine a new leader.  If the previous
// leader is able to re-establish a connection with the group, it
// detects that it has been usurped and becomes a follower.  The
// protocol also specifies how to bring the former leader back into
// a consistent state.
//
// Perhaps the most important aspect of Raft is its simplicity with
// respect to data flow design.
//
// * Updates flow from leader to follower. Period.
//
// This one property can make the management of a distributed log a very
// tractable problem, even when exposed directly to consumer machines.
//
// # Building State Machines
//
// Now that we've got some of the basics of distributed consensus out of
// the way, we can start looking at some basic machine designs. The
// machine and log interactions look like the following:
//
//
//                  *Local Process*                           |  *Network Process*
//                                                            |
//                               |-------*commit*------|      |
//                               v                     |      |
// {Consumer}---*updates*--->{Machine}---*append*--->{Log}<---+--->{Peer}
//                               |                     ^      |
//                               |-------*compact*-----|      |
//                                                            |
//
//
// This brings us to the main issue of machine design:
//
// * Appends and commits are separate, unsynchronized streams.
//
// Moreover, users of these APIs must not make any assumptions about the
// relationship of one stream to the other.  In other words, a successful
// append ONLY gives the guarantee that it has been committed to a majority
// of peers, and not necessarily to itself.  If the consuming machine requires
// strong consistency, synchronizing the request with the commit stream
// is likely required.
//
// # Log Compactions
//
// For many machines, their state is actually represented by many redundant
// log items.  The machine can provide the log of a shortened 'snapshot'
// version of itself using the Log#Compact() method.  This, however, means
// that the underlying log itself is pruned - or partially deleted.  For
// consumers of the log, this means that they must not make any assumptions
// about the state of the log with respect to their next read.  The log may
// be compacted at any time, for any reason.  Machines must be resilient to
// this.
//
// In practical terms, this mostly means that machines must be ready to rebuild
// their state at any time.
//
// # Linearizability
//
// For those unfamiliar with the term, linearizability is simply a property
// that states that given an object, any concurrent operation on that
// object must be equivalent to some legal sequential operation [2].
//
// When it comes to describing a log of state machine commands, linearizability
// means a few things:
//
//  * Items arrive in the order they were received.
//  * Items are never lost
//  * Items may arrive multiple times
//
// This implementation differs from raft in that it does NOT support full linearizability
// with respect to duplicate items.  Therefore, a requirement of every state
// machine must be idempotent. And just for good measure:
//
// * Every consuming state machine must be idempotent.
//
// However, an often overlooked aspect of linearizability is that concurrent
// operations also include reads.  Imagine a series of reads on some value
// within a state machine.  If those reads all happen on the same machine,
// it is very likely that no illegal sequence was witnessed.  However, take
// the same sequence of reads and execute them across several machines. As
// long as that sequence of reads never shows any out of date or partial
// changes, the system is said to be linearizable.
//
// Raft provides linearizability through the use of what are known as
// "read-barriers".  Machines which should not permit stale reads may
// use the syncing library to query for a read-barrier.  This represents
// the lowest entry that has been guaranteed to be applied to a majority
// of machines.
//
// [1] https://raft.github.io/raft.pdf
// [2] http://cs.brown.edu/~mph/HerlihyW90/p463-herlihy.pdf
//
package raft

import (
	"fmt"
	"io"
	"math"
	"os"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/enc"
)

// Core api errors
var (
	ErrClosed      = errors.New("Raft:ErrClosed")
	ErrCanceled    = errors.New("Raft:ErrCanceled")
	ErrNotLeader   = errors.New("Raft:ErrNotLeader")
	ErrNotFollower = errors.New("Raft:ErrNotFollower")
	ErrNoLeader    = errors.New("Raft:ErrNoLeader")
)

func StartBackground(addr string, fns ...Option) (Host, error) {
	ctx := context.NewContext(os.Stdout, context.Off)
	return Start(ctx, addr, fns...)
}

// Starts the first member of a raft cluster.  The given addr MUST be routable by external members
func Start(ctx context.Context, addr string, fns ...Option) (Host, error) {
	host, err := newHost(ctx, addr, buildOptions(fns...))
	if err != nil {
		return nil, errors.Wrap(err, "Unable to initialize host")
	}

	return host, host.start()
}

// Joins a newly initialized member to an existing raft cluster.
func Join(ctx context.Context, addr string, peers []string, fns ...Option) (Host, error) {
	host, err := newHost(ctx, addr, buildOptions(fns...))
	if err != nil {
		return nil, err
	}

	return host, host.join(peers)
}

type Kind int

var (
	Std  Kind = 0
	NoOp Kind = 1
	Conf Kind = 2
)

func (k Kind) String() string {
	switch k {
	default:
		return "Unknown"
	case Std:
		return "Std"
	case NoOp:
		return "NoOp"
	case Conf:
		return "Config"
	}
}

type Event []byte

func (e Event) Decode(dec enc.Decoder, ptr interface{}) error {
	return dec.DecodeBinary(e, ptr)
}

// An Entry represents an entry in the replicated log.
type Entry struct {
	Kind    Kind   `json:"kind"`
	Term    int64  `json:"term"`
	Index   int64  `json:"index"`
	Payload []byte `json:"payload"`
}

func (l Entry) String() string {
	return fmt.Sprintf("Entry(idx=%v,term=%v,kind=%v,size=%v)", l.Index, l.Term, l.Kind, len(l.Payload))
}

func (e Entry) parseConfig(dec enc.Decoder) (ret Config, err error) {
	err = dec.DecodeBinary(e.Payload, &ret)
	return
}

type Config struct {
	Peers Peers `json:"peers"`
}

func (c Config) encode(enc enc.Encoder) (ret []byte, err error) {
	err = enc.EncodeBinary(c, &ret)
	return
}

func parseConfig(dec enc.Decoder, data []byte) (ret Config, err error) {
	err = dec.DecodeBinary(data, &ret)
	return
}

// A host is a member of a cluster that is actively participating in log replication.
type Host interface {
	io.Closer

	// Returns the peer representing this host
	Self() Peer

	// Returns the peer objects of all members in the cluster.
	Roster() Peers

	// Returns the cluster synchronizer.
	Sync() (Sync, error)

	// Retrieves a connection to the log.
	Log() (Log, error)
}

type Log interface {

	// Returns the index of the maximum inserted item in the local log.
	Head() int64

	// Returns the index of the maximum committed item in the local log.
	Committed() int64

	// Returns the latest snaphot and the maximum index that the stream represents.
	Snapshot() (int64, EventStream, error)

	// Listen generates a stream of committed log entries starting at and
	// including the start index.
	//
	// This listener guarantees that entries are delivered with at-least-once
	// semantics.
	Listen(start int64, buf int64) (Listener, error)

	// Append appends and commits the event to the log.
	//
	// If the append is successful, do not assume that all committed items
	// have been replicated to this instance.  Appends should always accompany
	// a sync routine that ensures that the log has been caught up prior to
	// returning control.
	Append(cancel <-chan struct{}, event []byte) (Entry, error)

	// Compact replaces the log until the given point with the given snapshot
	//
	// Once the snapshot has been safely stored, the log until and including
	// the index will be deleted. This method is synchronous, but can be called
	// concurrent to other log methods. It should be considered safe for the
	// machine to continue to serve requests while a compaction is processing.
	//
	// Concurrent compactions are possible, however, an invariant of the log
	// is that it must always progress.  Therefore, an older snapshot cannot
	// usurp a newer one.
	Compact(until int64, snapshot <-chan Event) error
}

type Listener interface {
	io.Closer
	Ctrl() context.Control
	Data() <-chan Entry
}

type EventStream interface {
	io.Closer
	Ctrl() context.Control
	Data() <-chan Event
}

// Returns a channel that returns all the events in the batch.  The
// channel is closed once all items have been received by the channel
func newEventChannel(batch []Event) (ret <-chan Event) {
	ch := make(chan Event)
	go func() {
		for _, cur := range batch {
			ch <- cur
		}
		close(ch)
	}()
	ret = ch
	return
}

// The synchronizer gives the consuming machine the ability to synchronize
// its state with other members of the cluster.  This is critical for
// machines to be able to prevent stale reads.
//
// In order to give linearizable reads, consumers can query for a "read-barrier"
// index. This index is the maximum index that has been applied to any machine
// within the cluster.  With this barrier, machines can ensure their
// internal state has been caught up to the time the read was initiated,
// thereby obtaining linearizability for the operation.
type Sync interface {

	// Applied tells the synchronizer that the index (and everything that preceded)
	// it has been applied to the state machine.
	Ack(index int64)

	// Returns the current read-barrier for the cluster.
	Barrier(cancel <-chan struct{}) (int64, error)

	// Sync waits for the local machine to be caught up to the barrier.
	Sync(cancel <-chan struct{}, index int64) error
}

func min(l int64, others ...int64) int64 {
	min := l
	for _, o := range others {
		if o < min {
			min = o
		}
	}
	return min
}

func max(l int64, others ...int64) int64 {
	max := l
	for _, o := range others {
		if o > max {
			max = o
		}
	}
	return max
}

func majority(num int) int {
	if num%2 == 0 {
		return 1 + (num / 2)
	} else {
		return int(math.Ceil(float64(num) / float64(2)))
	}
}
