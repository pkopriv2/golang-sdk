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
package raft

import (
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	uuid "github.com/satori/go.uuid"
)

// Core api errors
var (
	ErrClosed    = errors.New("Raft:ErrClosed")
	ErrCanceled  = errors.New("Raft:ErrCanceled")
	ErrTooSlow   = errors.New("Raft:ErrTooSlow")
	ErrNotLeader = errors.New("Raft:ErrNotLeader")
	ErrNoLeader  = errors.New("Raft:ErrNoLeader")
	ErrNotConfig = errors.New("Raft:ErrNotConfig")
	ErrInvalid   = errors.New("Raft:ErrInvalid")
)

// Starts the first member of a raft cluster in the background.
// The provided address must be routable by external members.
func StartBackground(addr string, o ...Option) (Host, error) {
	return Start(context.NewContext(os.Stdout, context.Off), addr, o...)
}

// Joins a new peer to the existing raft cluster in the background.
// The provided address must be routable by external members.
func JoinBackground(addr string, peers []string, o ...Option) (Host, error) {
	return Join(context.NewContext(os.Stdout, context.Off), addr, peers, o...)
}

// Starts the first member of a raft cluster.  The given addr MUST be routable by external members
func Start(ctx context.Context, addr string, o ...Option) (Host, error) {
	host, err := newHost(ctx, addr, buildOptions(o))
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to start cluster [%v]", addr)
	}

	return host, host.start()
}

// Joins a newly initialized member to an existing raft cluster.
func Join(ctx context.Context, addr string, peers []string, o ...Option) (Host, error) {
	host, err := newHost(ctx, addr, buildOptions(o))
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to join cluster [%v]", peers)
	}

	return host, host.join(peers)
}

// A host is a member of a cluster that is actively participating in log replication.
type Host interface {
	io.Closer

	// Leaves the cluster and shuts down.
	Leave() error

	// Returns the peer representing this host
	Self() Peer

	// Returns the current term of the host
	Term() Term

	// Returns the peer objects of all members in the cluster.
	Roster() Peers

	// Returns the cluster synchronizer.
	Sync() (Sync, error)

	// Returns the cluster log
	Log() (Log, error)
}

// A log is a highly-reliable, strongly consistent distributed log.
type Log interface {
	io.Closer

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
	Compact(cancel <-chan struct{}, until int64, snapshot <-chan Event) error
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

// A listener allows consumers to receive entries from the log
type Listener interface {
	io.Closer
	Ctrl() context.Control
	Data() <-chan Entry
}

// An event stream allows consumers to send and receive snapshots
type EventStream interface {
	io.Closer
	Ctrl() context.Control
	Data() <-chan Event
}

// Kind describes the the type of entry in the log.  Consumers
// typically only need to worry about standard entries.
type Kind string

var (
	Std  Kind = "Std"
	Conf Kind = "Conf"
	NoOp Kind = "NoOp"
)

// An event represents the payload of an entry.
type Event []byte

func (e Event) Decode(dec enc.Decoder, ptr interface{}) error {
	return dec.DecodeBinary(e, ptr)
}

// An Entry represents an entry in the replicated log.
type Entry struct {
	Kind    Kind  `json:"kind"`
	Term    int64 `json:"term"`
	Index   int64 `json:"index"`
	Payload Event `json:"payload"`
}

func (l Entry) String() string {
	return fmt.Sprintf("Entry(idx=%v,term=%v,kind=%v,size=%v)", l.Index, l.Term, l.Kind, len(l.Payload))
}

func (e Entry) ParseConfig(dec enc.Decoder) (ret Config, err error) {
	if e.Kind != Conf {
		err = ErrNotConfig
		return
	}
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

// A term represents a host state in the Raft time model.
type Term struct {
	Epoch    int64      `json:"epoch"`     // the current term number (increases monotonically across the cluster)
	LeaderId *uuid.UUID `json:"leader_id"` // the current leader (as seen by this member)
	VotedFor *uuid.UUID `json:"voted_for"` // who was voted for this term (guaranteed not nil when leader != nil)
}

func (t Term) String() string {
	leaderId := "nil"
	if t.LeaderId != nil {
		leaderId = t.LeaderId.String()[:8]
	}

	votedFor := "nil"
	if t.VotedFor != nil {
		votedFor = t.VotedFor.String()[:8]
	}

	return fmt.Sprintf("Term(%v, l=%v, v=%v", t.Epoch, leaderId, votedFor)
}

// ---------------------- STORAGE APIS -----------------------------------
// The following APIs describe the storage layer. Consumers are free
// to swap or implement their own as necessary. This library currently
// ships with a badgerdb (https://github.com/dgraph-io/badger)
// implementation.
// -----------------------------------------------------------------------

var (
	ErrNoEntry     = errors.New("Raft:ErrNoEntry")
	ErrNoSnapshot  = errors.New("Raft:ErrNoSnapshot")
	ErrLogExists   = errors.New("Raft:ErrLogExists")
	ErrLogCorrupt  = errors.New("Raft:ErrLogCorrupt")
	ErrConflict    = errors.New("Raft:ErrConflict")
	ErrOutOfBounds = errors.New("Raft:ErrOutOfBounds")
)

// Stores information regarding the state of the peer and elections.
type PeerStorage interface {
	GetPeerId(addr string) (uuid.UUID, bool, error)
	SetPeerId(addr string, id uuid.UUID) error
	GetActiveTerm(peerId uuid.UUID) (Term, bool, error)
	SetActiveTerm(peerId uuid.UUID, term Term) error
}

// Stores information about logs and snapshots.
type LogStorage interface {
	GetLog(logId uuid.UUID) (DurableLog, error)
	NewLog(logId uuid.UUID, config Config) (DurableLog, error)
	InstallSnapshotSegment(snapshotId uuid.UUID, offset int64, batch []Event) error
	InstallSnapshot(snapshotId uuid.UUID, lastIndex int64, lastTerm int64, size int64, config Config) (DurableSnapshot, error)
}

// A StoredLog represents a durable log.
type DurableLog interface {
	Id() uuid.UUID
	Store() LogStorage
	LastIndexAndTerm() (index int64, term int64, err error)
	Scan(beg int64, end int64) ([]Entry, error)
	Append(data []byte, term int64, k Kind) (Entry, error)
	Get(index int64) (Entry, bool, error)
	Insert([]Entry) error
	Install(DurableSnapshot) error
	Snapshot() (DurableSnapshot, error)
}

// A Snapshot represents the log up to a certain index.  The
// snapshots themselves consist of a sequence of events.
//
// TODO: Consider using a more generalized structure to allow
// for arbitrary documents.
type DurableSnapshot interface {
	Id() uuid.UUID
	LastIndex() int64
	LastTerm() int64
	Size() int64
	Config() Config
	Scan(beg int64, end int64) ([]Event, error)
	Delete() error
}

// ---------------------- TRANSPORT APIS ---------------------------------
// The following APIs describe the transport layer APIs. Consumers are free
// to swap or implement their own as necessary. This library currently ships
// with a simple rpc implementation.
// -----------------------------------------------------------------------

// The transport is the primary interface that describes how peers communicate.
type Transport interface {

	// Returns a new socket bound to the given address.
	Listen(addr string) (Socket, error)

	// Returns a client connected to the given addr.
	Dial(addr string, timeout time.Duration) (ClientSession, error)
}

// Spawns new server sessions.
type Socket interface {
	io.Closer

	// Returns the local address of the socket.
	Addr() string

	// Returns a new server session.  Blocks until one is available.
	Accept() (ServerSession, error)
}

// Manages the lifecycle of a single server connection.
type ServerSession interface {
	io.Closer

	// Returns the local address of the session.
	LocalAddr() string

	// Returns the remote address of the session.
	RemoteAddr() string

	// Returns the next request from the session.  The returned value
	// must be one of:
	//
	//  * StatusRequest
	//  * ReadBarrierRequest
	//  * RosterUpdateRequest
	//  * ReplicateRequest
	//  * VoteRequest
	//  * AppendRequest
	//  * InstallSnapshotRequest
	//
	ReadRequest(time.Duration) (interface{}, error)

	// Sends a response to the client.  Must be one of:
	//
	//  * nil
	//  * error
	//  * StatusResponse
	//  * ReadBarrierResponse
	//  * RosterUpdateResponse
	//  * ReplicateResponse
	//  * VoteResponse
	//  * AppendResponse
	//  * InstallSnapshotResponse
	//
	SendResponse(interface{}, time.Duration) error
}

// The core client interface.
type ClientSession interface {
	io.Closer

	// Sends a request to the destination server.  The input must be one of:
	//
	//  * StatusRequest
	//  * ReadBarrierRequest
	//  * RosterUpdateRequest
	//  * ReplicateRequest
	//  * VoteRequest
	//  * AppendRequest
	//  * InstallSnapshotRequest
	//
	SendRequest(interface{}, time.Duration) error

	// Reads a response from the server.  The input must be one of:
	//
	//  * *StatusResponse
	//  * *ReadBarrierResponse
	//  * *RosterUpdateResponse
	//  * *ReplicateResponse
	//  * *VoteResponse
	//  * *AppendResponse
	//  * *InstallSnapshotResponse
	//
	ReadResponse(interface{}, time.Duration) error
}

type StatusRequest struct{}

type StatusResponse struct {
	Self   Peer   `json:"self"`
	Term   Term   `json:"term"`
	Config Config `json:"config"`
}

type ReadBarrierRequest struct{}

type ReadBarrierResponse struct {
	Barrier int64 `json:"barrier"`
}

type RosterUpdateRequest struct {
	Peer Peer `json:"peer"`
	Join bool `json:"join"`
}

type RosterUpdateResponse struct{}

type ReplicateRequest struct {
	LeaderId     uuid.UUID `json:"leader_id"`
	Term         int64     `json:"term"`
	PrevLogIndex int64     `json:"prev_log_index"`
	PrevLogTerm  int64     `json:"prev_log_term"`
	Items        []Entry   `json:"items"`
	Commit       int64     `json:"commit"`
}

type ReplicateResponse struct {
	Term    int64 `json:"term"`
	Success bool  `json:"success"`
	Hint    int64 `json:"hint"` // used for fast agreemnt
}

type VoteRequest struct {
	Id          uuid.UUID `json:"id"`
	Term        int64     `json:"term"`
	MaxLogIndex int64     `json:"max_log_index"`
	MaxLogTerm  int64     `json:"max_log_term"`
}

type VoteResponse struct {
	Term    int64 `json:"term"`
	Granted bool  `json:"granted"`
}

type AppendRequest struct {
	Event []byte `json:"event"`
	Kind  Kind   `json:"kind"`
}

type AppendResponse struct {
	Index int64 `json:"index"`
	Term  int64 `json:"term"`
}

type InstallSnapshotRequest struct {
	LeaderId    uuid.UUID `json:"leader_id"`
	Id          uuid.UUID `json:"id"`
	Term        int64     `json:"term"`
	Config      Config    `json:"config"`
	Size        int64     `json:"size"`
	MaxIndex    int64     `json:"max_index"`
	MaxTerm     int64     `json:"max_term"`
	BatchOffset int64     `json:"batch_offset"`
	Batch       []Event   `json:"batch"`
}

type InstallSnapshotResponse struct {
	Term    int64 `json:"term"`
	Success bool  `json:"success"`
}

func newHeartBeat(id uuid.UUID, term int64, commit int64) ReplicateRequest {
	return ReplicateRequest{id, term, -1, -1, []Entry{}, commit}
}

func newReplication(id uuid.UUID, term int64, prevIndex int64, prevTerm int64, items []Entry, commit int64) ReplicateRequest {
	return ReplicateRequest{id, term, prevIndex, prevTerm, items, commit}
}

// Installs a new snapshot of unknown size from a channel of events
func newSnapshot(store LogStorage, lastIndex, lastTerm int64, config Config, data <-chan Event, cancel <-chan struct{}) (ret DurableSnapshot, err error) {
	snapshotId := uuid.NewV1()

	offset := int64(0)
	for {
		batch := make([]Event, 0, 256)
		for i := 0; i < 256; i++ {
			select {
			case <-cancel:
				err = ErrCanceled
				return
			case entry, ok := <-data:
				if !ok {
					break
				}

				batch = append(batch, entry)
			}
		}

		if len(batch) == 0 {
			break
		}

		if err = store.InstallSnapshotSegment(snapshotId, offset, batch); err != nil {
			err = errors.Wrapf(err, "Unable to install snapshot segment [offset=%v,num=%v]", offset, len(batch))
			return
		}

		offset += int64(len(batch))
	}

	ret, err = store.InstallSnapshot(snapshotId, lastIndex, lastTerm, offset, config)
	return
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
