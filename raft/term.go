package raft

import (
	badger "github.com/dgraph-io/badger/v3"
	"github.com/pkopriv2/golang-sdk/lang/bin"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	uuid "github.com/satori/go.uuid"
)

var (
	termPrefix   = bin.String("term")
	termIdPrefix = bin.String("term.id")
)

type BadgerPeerStorage struct {
	db *badger.DB
}

func NewBadgerPeerStorage(db *badger.DB) PeerStorage {
	return &BadgerPeerStorage{db}
}

func (t *BadgerPeerStorage) GetPeerId(addr string) (id uuid.UUID, ok bool, err error) {
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

func (t *BadgerPeerStorage) SetPeerId(addr string, id uuid.UUID) error {
	return t.db.Update(func(tx *badger.Txn) error {
		return tx.Set(termIdPrefix.String(addr), id.Bytes())
	})
}

func (t *BadgerPeerStorage) GetActiveTerm(id uuid.UUID) (term Term, ok bool, err error) {
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

func (t *BadgerPeerStorage) SetActiveTerm(id uuid.UUID, term Term) error {
	return t.db.Update(func(tx *badger.Txn) (err error) {
		bytes, err := enc.Encode(enc.Json, term)
		if err != nil {
			return
		}
		return tx.Set(bin.UUID(id), bytes)
	})
}
