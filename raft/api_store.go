package raft

import (
	"errors"

	uuid "github.com/satori/go.uuid"
)

// Durability apis

// TODO: Complete the api so that we can have command line utilities for interacting
// with nodes.

// Storage api errors
var (
	ErrMissingEntry = errors.New("Raft:ErrMissingEntry")
	ErrInvariant    = errors.New("Raft:ErrInvariant")
	ErrOutOfBounds  = errors.New("Raft:ErrOutOfBounds")
	ErrCompaction   = errors.New("Raft:ErrCompaction")
)

type LogStore interface {
	GetLog(uuid.UUID) (StoredLog, error)
	NewLog(uuid.UUID, Config) (StoredLog, error)

	// FIXME: snapshotting is currently broken. Need to be able to install it in chunks
	NewSnapshot(lastIndex int64, lastTerm int64, data <-chan Event, conf Config) (StoredSnapshot, error)
}

type StoredLog interface {
	Id() uuid.UUID
	Store() LogStore
	LastIndexAndTerm() (max int64, term int64, err error)
	TrimRight(start int64) error
	TrimLeft(end int64) error
	Scan(beg int64, end int64) ([]Entry, error)
	Append(data []byte, term int64, k Kind) (Entry, error)
	Get(index int64) (Entry, bool, error)
	Insert([]Entry) error
	Install(StoredSnapshot) error
	Snapshot() (StoredSnapshot, error)
}

type StoredSnapshot interface {
	LastIndex() int64
	LastTerm() int64
	Size() int64
	Config() Config
	Scan(beg int64, end int64) ([]Event, error)
	Delete() error
}
