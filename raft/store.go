package raft

import (
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// Durability apis

// TODO: Complete the api so that we can have command line utilities for interacting
// with nodes.

// Storage api errors
var (
	ErrNoEntry     = errors.New("Raft:ErrNoEntry")
	ErrNoSnapshot  = errors.New("Raft:ErrNoSnapshot")
	ErrInvariant   = errors.New("Raft:ErrInvariant")
	ErrOutOfBounds = errors.New("Raft:ErrOutOfBounds")
	ErrCompaction  = errors.New("Raft:ErrCompaction")
)

type TermStore interface {
	GetPeerId(addr string) (uuid.UUID, bool, error)
	SetPeerId(addr string, id uuid.UUID) error
	GetActiveTerm(peerId uuid.UUID) (Term, bool, error)
	SetActiveTerm(peerId uuid.UUID, term Term) error
}

type LogStore interface {
	GetLog(logId uuid.UUID) (StoredLog, error)
	NewLog(logId uuid.UUID, config Config) (StoredLog, error)
	InstallSnapshotSegment(snapshotId uuid.UUID, offset int64, batch []Event) error
	InstallSnapshot(snapshotId uuid.UUID, lastIndex int64, lastTerm int64, size int64, config Config) (StoredSnapshot, error)
}

// Installs a new snapshot of unknown size from a channel events events
func newSnapshot(store LogStore, lastIndex, lastTerm int64, config Config, data <-chan Event, cancel <-chan struct{}) (ret StoredSnapshot, err error) {
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

type StoredLog interface {
	Id() uuid.UUID
	Store() LogStore
	LastIndexAndTerm() (max int64, term int64, err error)
	Scan(beg int64, end int64) ([]Entry, error)
	Append(data []byte, term int64, k Kind) (Entry, error)
	Get(index int64) (Entry, bool, error)
	Insert([]Entry) error
	Install(StoredSnapshot) error
	Snapshot() (StoredSnapshot, error)
}

type StoredSnapshot interface {
	Id() uuid.UUID
	LastIndex() int64
	LastTerm() int64
	Size() int64
	Config() Config
	Scan(beg int64, end int64) ([]Event, error)
	Delete() error
}
