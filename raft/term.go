package raft

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/pkopriv2/golang-sdk/lang/bin"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	uuid "github.com/satori/go.uuid"
)

// A term represents a particular member state in the Raft epochal time model.
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

var (
	termPrefix   = bin.String("term")
	termIdPrefix = bin.String("term.id")
)

type BadgerTermStore struct {
	db *badger.DB
}

func NewBadgerTermStore(db *badger.DB) TermStore {
	return &BadgerTermStore{db}
}

func (t *BadgerTermStore) GetPeerId(addr string) (id uuid.UUID, ok bool, err error) {
	err = t.db.View(func(tx *badger.Txn) (err error) {
		item, err := tx.Get(termIdPrefix.String(addr))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				err = nil
			}
			return
		}

		bytes, err := item.ValueCopy(nil)
		if err != nil {
			return
		}

		id, err = uuid.FromBytes(bytes)
		if err != nil {
			return
		}

		ok = true
		return
	})
	return
}

func (t *BadgerTermStore) SetPeerId(addr string, id uuid.UUID) error {
	return t.db.Update(func(tx *badger.Txn) error {
		return tx.Set(termIdPrefix.String(addr), id.Bytes())
	})
}

func (t *BadgerTermStore) GetActiveTerm(id uuid.UUID) (term Term, ok bool, err error) {
	err = t.db.View(func(tx *badger.Txn) (err error) {
		item, err := tx.Get(termPrefix.UUID(id))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				err = nil
			}
		}
		return

		bytes, err := item.ValueCopy(nil)
		if err != nil {
			return
		}

		ok, err = true, enc.Json.DecodeBinary(bytes, &term)
		return
	})
	return
}

func (t *BadgerTermStore) SetActiveTerm(id uuid.UUID, term Term) error {
	return t.db.Update(func(tx *badger.Txn) (err error) {
		bytes, err := enc.Encode(enc.Json, term)
		if err != nil {
			return
		}
		return tx.Set(bin.UUID(id), bytes)
	})
}
