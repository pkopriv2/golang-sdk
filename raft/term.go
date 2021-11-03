package raft

import (
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	uuid "github.com/satori/go.uuid"
)

// A term represents a particular member state in the Raft epochal time model.
type term struct {
	Num      int64      `json:"num"`       // the current term number (increases monotonically across the cluster)
	LeaderId *uuid.UUID `json:"leader_id"` // the current leader (as seen by this member)
	VotedFor *uuid.UUID `json:"voted_for"` // who was voted for this term (guaranteed not nil when leader != nil)
}

func (t term) String() string {
	leaderId := "nil"
	if t.LeaderId != nil {
		leaderId = t.LeaderId.String()[:8]
	}

	votedFor := "nil"
	if t.LeaderId != nil {
		votedFor = t.VotedFor.String()[:8]
	}

	return fmt.Sprintf("Term(%v, l=%v, v=%v", t.Num, leaderId, votedFor)
}

var (
	termBucket   = []byte("raft.term")
	termIdBucket = []byte("raft.term.id")
)

func initBoltTermBucket(tx *bolt.Tx) error {
	_, e1 := tx.CreateBucketIfNotExists(termBucket)
	_, e2 := tx.CreateBucketIfNotExists(termIdBucket)
	return errs.Or(e1, e2)
}

type TermStore struct {
	db *bolt.DB
}

func NewTermStore(db *bolt.DB) (*TermStore, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		return initBoltTermBucket(tx)
	})
	if err != nil {
		return nil, err
	}
	return &TermStore{db}, nil
}

func (t *TermStore) GetId(addr string) (id uuid.UUID, ok bool, err error) {
	err = t.db.View(func(tx *bolt.Tx) (err error) {
		bytes := tx.Bucket(termIdBucket).Get([]byte(addr))
		if bytes == nil {
			return
		}

		id, err = uuid.FromBytes(tx.Bucket(termIdBucket).Get([]byte(addr)))
		if err != nil {
			return
		}

		ok = true
		return
	})
	return
}

func (t *TermStore) SetId(addr string, id uuid.UUID) error {
	return t.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(termIdBucket).Put([]byte(addr), id.Bytes())
	})
}

func (t *TermStore) Get(id uuid.UUID) (term term, ok bool, err error) {
	var bytes []byte
	err = t.db.View(func(tx *bolt.Tx) error {
		bytes = tx.Bucket(termBucket).Get(id.Bytes())
		return nil
	})
	if bytes != nil {
		if err = enc.Json.DecodeBinary(bytes, &term); err != nil {
			return
		}

		ok = true
	}
	return
}

func (t *TermStore) Save(id uuid.UUID, tm term) error {
	var bytes []byte
	if err := enc.Json.EncodeBinary(tm, &bytes); err != nil {
		return err
	}
	return t.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(termBucket).Put(id.Bytes(), bytes)
	})
}
