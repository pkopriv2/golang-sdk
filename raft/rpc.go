package raft

import (
	uuid "github.com/satori/go.uuid"
)

// server endpoints
const (
	funcReadBarrier     = "raft.readBarrier"
	funcStatus          = "raft.status"
	funcRequestVote     = "raft.requestVote"
	funcUpdateRoster    = "raft.roster"
	funcReplicate       = "raft.replicate"
	funcAppend          = "raft.append"
	funcInstallSnapshot = "raft.snapshot"
)

type statusResponse struct {
	Self   Peer   `json:"self"`
	Term   term   `json:"term"`
	Config Config `json:"config"`
}

type readBarrierResponse struct {
	Barrier int64 `json:"barrier"`
}

type rosterUpdateRequest struct {
	Peer Peer `json:"peer"`
	Join bool `json:"join"`
}

// Internal append events request.  Requests are put onto the internal member
// channel and consumed by the currently active sub-machine.
//
// Append events ONLY come from members who are leaders. (Or think they are leaders)
type replicateRequest struct {
	LeaderId     uuid.UUID `json:"leader_id"`
	Term         int64     `json:"term"`
	PrevLogIndex int64     `json:"prev_log_index"`
	PrevLogTerm  int64     `json:"prev_log_term"`
	Items        []Entry   `json:"items"`
	Commit       int64     `json:"commit"`
}

// Internal response type.  These are returned through the
// request 'ack'/'response' channels by the currently active
// sub-machine component.
type replicateResponse struct {
	Term    int64 `json:"term"`
	Success bool  `json:"success"`
	Hint    int64 `json:"hint"` // used for fast agreemnt
}

func newHeartBeat(id uuid.UUID, term int64, commit int64) replicateRequest {
	return replicateRequest{id, term, -1, -1, []Entry{}, commit}
}

func newReplication(id uuid.UUID, term int64, prevIndex int64, prevTerm int64, items []Entry, commit int64) replicateRequest {
	return replicateRequest{id, term, prevIndex, prevTerm, items, commit}
}

// Internal request vote.  Requests are put onto the internal member
// channel and consumed by the currently active sub-machine.
//
// Request votes ONLY come from members who are candidates.
type voteRequest struct {
	Id          uuid.UUID `json:"id"`
	Term        int64     `json:"term"`
	MaxLogIndex int64     `json:"max_log_index"`
	MaxLogTerm  int64     `json:"max_log_term"`
}

type voteResponse struct {
	Term    int64 `json:"term"`
	Granted bool  `json:"granted"`
}

// Client append request.  Requests are put onto the internal member
// channel and consumed by the currently active sub-machine.
//
// These come from active clients.
type appendEventRequest struct {
	Event []byte `json:"event"`
	Kind  Kind   `json:"kind"`
}

// append event response type.
type appendEventResponse struct {
	Index int64 `json:"index"`
	Term  int64 `json:"term"`
}

type installSnapshotRequest struct {
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

type installSnapshotResponse struct {
	Term    int64 `json:"term"`
	Success bool  `json:"success"`
}
