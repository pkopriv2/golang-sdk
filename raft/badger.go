package raft

import (
	badger "github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/bin"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	uuid "github.com/satori/go.uuid"
)

// Badger implementation of raft log store.
var (
	logPrefix            = bin.String("log")
	logEntryPrefix       = bin.String("log.entry")
	logMinPrefix         = bin.String("log.min")          // holds the beginning index of the log. (key = /<logId>)
	logMaxPrefix         = bin.String("log.max")          // holds the ending index of the log. (key = /<logId>)
	logSnapshotPrefix    = bin.String("log.snapshot")     // holds the uuid of the active snapshot. (key = /<logId>)
	snapshotsPrefix      = bin.String("snapshots")        // holds the snapshot summary (key = /<snapshotId>)
	snapshotEventsPrefix = bin.String("snapshots.events") // holds the snapshot events (key = /<snapshotId>)
)

// Store impl.
type BadgerStore struct {
	db *badger.DB
}

func NewBadgerLogStore(db *badger.DB) LogStore {
	return &BadgerStore{db}
}

func (s *BadgerStore) GetLog(id uuid.UUID) (StoredLog, error) {
	log, err := badgerOpenLog(s.db, id)
	if err != nil || log == nil {
		return nil, err
	}
	return log, nil
}

func (s *BadgerStore) NewLog(id uuid.UUID, config Config) (StoredLog, error) {
	return badgerCreateLog(s.db, id, config)
}

func (s *BadgerStore) NewSnapshot(cancel <-chan struct{}, lastIndex int64, lastTerm int64, ch <-chan Event, config Config) (StoredSnapshot, error) {
	return badgerCreateSnapshot(s.db, lastIndex, lastTerm, ch, cancel, config)
}

func (s *BadgerStore) InstallSnapshot(snapshotId uuid.UUID, lastIndex int64, lastTerm int64, size int64, conf Config) (ret StoredSnapshot, err error) {
	return badgerInstallSnapshot(s.db, snapshotId, lastIndex, lastTerm, size, conf)
}

func (s *BadgerStore) InstallSnapshotSegment(snapshotId uuid.UUID, offset int64, batch []Event) error {
	return s.db.Update(func(tx *badger.Txn) error {
		return badgerPutSnapshotEvents(tx, snapshotId, offset, batch)
	})
}

// Parent log abstraction
type BadgerLog struct {
	db *badger.DB
	id uuid.UUID
}

func badgerCreateLog(db *badger.DB, id uuid.UUID, config Config) (log *BadgerLog, err error) {
	s, err := badgerCreateEmptySnapshot(db, config)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			s.Delete()
		}
	}()

	err = db.Update(func(tx *badger.Txn) error {
		if ok, _ := badgerLogExists(tx, id); ok {
			return errors.Wrapf(ErrInvariant, "Log [%v] already exists", id)
		} else {
			return badgerInitLog(tx, id, s.Id())
		}
	})
	if err != nil {
		return nil, err
	}

	return &BadgerLog{db, id}, nil
}

func badgerOpenLog(db *badger.DB, id uuid.UUID) (log *BadgerLog, err error) {
	err = db.View(func(tx *badger.Txn) error {
		ok, err := badgerLogExists(tx, id)
		if err != nil || !ok {
			return err
		}

		log = &BadgerLog{db, id}
		return nil
	})
	return
}

func (b *BadgerLog) Id() uuid.UUID {
	return b.id
}

func (b *BadgerLog) Store() LogStore {
	return &BadgerStore{b.db}
}

func (b *BadgerLog) Min() (m int64, e error) {
	e = b.db.View(func(tx *badger.Txn) error {
		m, e = badgerGetMinIndex(tx, b.Id())
		return e
	})
	return
}

func (b *BadgerLog) Max() (m int64, e error) {
	e = b.db.View(func(tx *badger.Txn) error {
		m, e = badgerGetMaxIndex(tx, b.Id())
		return e
	})
	return
}

func (b *BadgerLog) LastIndexAndTerm() (i int64, t int64, e error) {
	e = b.db.View(func(tx *badger.Txn) error {
		i, t, e = badgerGetMaxIndexAndTerm(tx, b.Id())
		return e
	})
	return
}

func (b *BadgerLog) TrimLeft(end int64) error {
	return b.db.Update(func(tx *badger.Txn) (e error) {
		return badgerTrimLeftLogEntries(tx, b.Id(), end)
	})
}

func (b *BadgerLog) Scan(beg int64, end int64) (i []Entry, e error) {
	e = b.db.View(func(tx *badger.Txn) error {
		i, e = badgerGetLogEntries(tx, b.Id(), beg, end)
		return e
	})
	return
}

func (b *BadgerLog) Append(event []byte, term int64, kind Kind) (i Entry, e error) {
	for {
		e = b.db.Update(func(tx *badger.Txn) error {
			i, e = badgerAppendLogEntry(tx, b.Id(), event, term, kind)
			return e
		})
		if e == nil || e != badger.ErrConflict {
			return
		}
	}
}

func (b *BadgerLog) Get(index int64) (i Entry, o bool, e error) {
	e = b.db.View(func(tx *badger.Txn) error {
		i, o, e = badgerGetLogEntry(tx, b.Id(), index)
		return e
	})
	return
}

func (b *BadgerLog) Insert(batch []Entry) error {
	err := b.db.Update(func(tx *badger.Txn) error {
		return badgerPutLogEntries(tx, b.Id(), batch)
	})
	return err
}

func (b *BadgerLog) SnapshotId() (i uuid.UUID, e error) {
	e = b.db.View(func(tx *badger.Txn) error {
		i, e = badgerGetActiveSnapshotId(tx, b.Id())
		return e
	})
	return
}

func (b *BadgerLog) Snapshot() (StoredSnapshot, error) {
	id, err := b.SnapshotId()
	if err != nil {
		return nil, err
	}

	return badgerOpenSnapshot(b.db, id)
}

func (b *BadgerLog) Install(s StoredSnapshot) error {
	cur, err := b.Snapshot()
	if err != nil {
		return err
	}

	if cur.Id() == s.Id() {
		return nil
	}
	// swap it.
	err = b.db.Update(func(tx *badger.Txn) error {
		return badgerSwapActiveSnapshotId(tx, b.Id(), cur.(*BadgerSnapshot).raw, s.(*BadgerSnapshot).raw)
	})
	if err != nil {
		return err
	}

	// finally, truncate (safe to do concurrently)
	err = b.TrimLeft(s.LastIndex())
	if err != nil {
		return err
	}

	return cur.Delete()
}

type BadgerSnapshot struct {
	db  *badger.DB
	raw badgerSnapshotSummary
}

func badgerCreateEmptySnapshot(db *badger.DB, config Config) (*BadgerSnapshot, error) {
	return badgerCreateSnapshot(db, -1, -1, newEventChannel([]Event{}), nil, config)
}

func badgerInstallSnapshot(db *badger.DB, snapshotId uuid.UUID, lastIndex int64, lastTerm int64, size int64, config Config) (ret *BadgerSnapshot, err error) {
	summary := badgerSnapshotSummary{snapshotId, lastIndex, lastTerm, size, config}
	err = db.Update(func(tx *badger.Txn) error {
		return badgerPutSnapshotSummary(tx, summary)
	})
	if err != nil {
		return
	}

	ret = &BadgerSnapshot{db, summary}
	return
}

// FIXME: Cleanup erroneous snapshot installations
func badgerCreateSnapshot(db *badger.DB, lastIndex int64, lastTerm int64, ch <-chan Event, cancel <-chan struct{}, config Config) (ret *BadgerSnapshot, err error) {
	snapshotId := uuid.NewV1()

	num, err := badgerPutSnapshotChannel(db, snapshotId, ch, cancel)
	if err != nil {
		return
	}

	summary := badgerSnapshotSummary{snapshotId, lastIndex, lastTerm, num, config}
	err = db.Update(func(tx *badger.Txn) error {
		return badgerPutSnapshotSummary(tx, summary)
	})
	if err != nil {
		return
	}

	ret = &BadgerSnapshot{db, summary}
	return
}

func badgerOpenSnapshot(db *badger.DB, id uuid.UUID) (ret *BadgerSnapshot, err error) {
	var raw badgerSnapshotSummary
	var ok bool
	err = db.View(func(tx *badger.Txn) error {
		raw, ok, err = badgerGetSnapshotSummary(tx, id)
		return err
	})
	if !ok || err != nil {
		err = errs.Or(err, errors.Wrapf(ErrNoSnapshot, "Missing snapshot [%v]", id))
		return
	}

	ret = &BadgerSnapshot{db, raw}
	return
}

func (b *BadgerSnapshot) Id() uuid.UUID {
	return b.raw.Id
}

func (b *BadgerSnapshot) Size() int64 {
	return b.raw.Size
}

func (b *BadgerSnapshot) Config() Config {
	return b.raw.Config
}

func (b *BadgerSnapshot) LastIndex() int64 {
	return b.raw.MaxIndex
}

func (b *BadgerSnapshot) LastTerm() int64 {
	return b.raw.MaxTerm
}

func (b *BadgerSnapshot) Delete() error {
	// FIXME: Probably need to atomically delete a snapshot.
	err := b.db.Update(func(tx *badger.Txn) error {
		return badgerDelSnapshotSummary(tx, b.raw.Id)
	})

	return errs.Or(err, badgerDelSnapshotEvents(b.db, b.raw.Id))
}

func (b *BadgerSnapshot) Scan(start int64, end int64) (batch []Event, err error) {
	err = b.db.View(func(tx *badger.Txn) (err error) {
		batch, err = badgerGetSnapshotEvents(tx, b.raw.Id, start, end)
		return
	})
	return
}

type badgerSnapshotSummary struct {
	Id       uuid.UUID `json:"id"`
	MaxIndex int64     `json:"max_index"`
	MaxTerm  int64     `json:"max_term"`
	Size     int64     `json:"size"`
	Config   Config    `json:"config"`
}

var (
	ErrKeyNotFound = badger.ErrKeyNotFound
)

func badgerGetSnapshotSummary(tx *badger.Txn, snapshotId uuid.UUID) (ret badgerSnapshotSummary, ok bool, err error) {
	item, err := tx.Get(snapshotsPrefix.UUID(snapshotId))
	if err != nil {
		if err == ErrKeyNotFound {
			err = nil
		}
		return
	}

	err = item.Value(func(raw []byte) (err error) {
		ok, err = true, enc.Json.DecodeBinary(raw, &ret)
		return
	})
	return
}

func badgerPutSnapshotSummary(tx *badger.Txn, summary badgerSnapshotSummary) (err error) {
	bytes, err := enc.Encode(enc.Json, summary)
	if err != nil {
		return
	}

	err = tx.Set(snapshotsPrefix.UUID(summary.Id), bytes)
	return
}

func badgerDelSnapshotSummary(tx *badger.Txn, snapshotId uuid.UUID) error {
	return tx.Delete(snapshotsPrefix.UUID(snapshotId))
}

func badgerGetSnapshotEvent(tx *badger.Txn, snapshotId uuid.UUID, idx int64) (ret Event, ok bool, err error) {
	item, err := tx.Get(snapshotEventsPrefix.UUID(snapshotId).Int64(idx))
	if err != nil {
		if err == ErrKeyNotFound {
			err = nil
		}
		return
	}

	if ret, err = item.ValueCopy(nil); err != nil {
		return
	}

	ok = true
	return
}

func badgerPutSnapshotEvent(tx *badger.Txn, snapshotId uuid.UUID, idx int64, e Event) error {
	return tx.Set(snapshotEventsPrefix.UUID(snapshotId).Int64(idx), e)
}

func badgerPutSnapshotEvents(tx *badger.Txn, snapshotId uuid.UUID, offset int64, batch []Event) (err error) {
	for i, e := range batch {
		if err = badgerPutSnapshotEvent(tx, snapshotId, offset+int64(i), e); err != nil {
			return
		}
	}
	return nil
}

// Stores the snapshot event stream into the db.  This implementation breaks the work
// into chunks to prevent blocking of concurrent reads/writes
func badgerPutSnapshotChannel(db *badger.DB, snapshotId uuid.UUID, ch <-chan Event, cancel <-chan struct{}) (num int64, err error) {
	num = 0
	for {
		chunk := make([]Event, 0, 1024) // TODO: could implement using reusable buffer
		for i := 0; i < 1024; i++ {
			select {
			case <-cancel:
				err = ErrCanceled
				return
			case e, ok := <-ch:
				if !ok {
					break
				}

				chunk = append(chunk, e)
			}

		}

		if len(chunk) == 0 {
			return
		}

		err = db.Update(func(tx *badger.Txn) (err error) {
			return badgerPutSnapshotEvents(tx, snapshotId, num, chunk)
		})
		if err != nil {
			return
		}

		num += int64(len(chunk))
	}
}

// Scans a range of the snapshot - [start, end)
func badgerGetSnapshotEvents(tx *badger.Txn, snapshotId uuid.UUID, start, end int64) (ret []Event, err error) {
	if start < 0 {
		err = errors.Wrapf(ErrOutOfBounds, "Invalid start [%v] when scanning snapshot events", start)
		return
	}

	ret = make([]Event, 0, end-start)
	for cur := start; cur < end; cur++ {
		e, ok, err := badgerGetSnapshotEvent(tx, snapshotId, cur)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to get snapshot event [%v] for snapshot [%v]", cur, snapshotId)
		}
		if !ok {
			return nil, errors.Wrapf(ErrNoEntry, "Missing expected entry [%v] for snapshot [%v]", cur, snapshotId)
		}
		ret = append(ret, e)
	}
	return
}

// Deletes the given snapshot's event stream.  This implementation breaks the work
// into chunks to prevent blocking of concurrent reads/writes
func badgerDelSnapshotEvents(db *badger.DB, snapshotId uuid.UUID) (err error) {
	prefix := snapshotEventsPrefix.UUID(snapshotId)

	for contd := true; contd; {
		err = db.Update(func(tx *badger.Txn) error {
			iter := tx.NewIterator(badger.DefaultIteratorOptions)

			dead := make([][]byte, 0, 1024)
			iter.Seek(prefix.Int64(0))
			for i := 0; i < 1024; i++ {
				if !iter.ValidForPrefix(prefix) {
					contd = false
					break
				}

				dead = append(dead, iter.Item().Key())
				iter.Next()
			}

			iter.Close()
			for _, i := range dead {
				if err := tx.Delete(i); err != nil {
					return err
				}
			}

			return nil
		})
		if err != nil {
			return err
		}
	}
	return
}

func badgerGetMaxIndex(tx *badger.Txn, logId uuid.UUID) (val int64, err error) {
	item, err := tx.Get(logMaxPrefix.UUID(logId))
	if err != nil {
		if err == ErrKeyNotFound {
			err = errors.Wrapf(ErrInvariant, "Missing max index for log [%v]", logId)
		}
		return
	}
	bytes, err := item.ValueCopy(nil)
	if err != nil {
		return
	}
	return bin.ParseInt64(bytes)
}

func badgerSetMaxIndex(tx *badger.Txn, logId uuid.UUID, max int64) error {
	return tx.Set(logMaxPrefix.UUID(logId), bin.Int64(max))
}

func badgerGetMinIndex(tx *badger.Txn, logId uuid.UUID) (val int64, err error) {
	item, err := tx.Get(logMinPrefix.UUID(logId))
	if err != nil {
		if err == ErrKeyNotFound {
			err = errors.Wrapf(ErrInvariant, "Missing max index for log [%v]", logId)
		}
		return
	}
	bytes, err := item.ValueCopy(nil)
	if err != nil {
		return
	}
	return bin.ParseInt64(bytes)
}

func badgerSetMinIndex(tx *badger.Txn, logId uuid.UUID, min int64) error {
	return tx.Set(logMinPrefix.UUID(logId), bin.Int64(min))
}

func badgerGetActiveSnapshotId(tx *badger.Txn, logId uuid.UUID) (ret uuid.UUID, err error) {
	item, err := tx.Get(logSnapshotPrefix.UUID(logId))
	if err != nil {
		if err == ErrKeyNotFound {
			err = errors.Wrapf(ErrInvariant, "Missing max index for log [%v]", logId)
		}
		return
	}
	bytes, err := item.ValueCopy(nil)
	if err != nil {
		return
	}
	return uuid.FromBytes(bytes)
}

func badgerSetActiveSnapshotId(tx *badger.Txn, logId, snapshotId uuid.UUID) error {
	return tx.Set(logSnapshotPrefix.UUID(logId), bin.UUID(snapshotId))
}

func badgerSwapActiveSnapshotId(tx *badger.Txn, logId uuid.UUID, prev, next badgerSnapshotSummary) error {
	curId, e := badgerGetActiveSnapshotId(tx, logId)
	if e != nil {
		return e
	}

	if curId != prev.Id {
		return errors.Wrapf(ErrInvariant, "Cannot swap snapshot [%v] with current [%v].  Current is no longer active.", next, prev)
	}

	cur, ok, err := badgerGetSnapshotSummary(tx, curId)
	if err != nil || !ok {
		return errs.Or(err, errors.Wrapf(ErrInvariant, "Missing snapshot [%v]", curId))
	}

	if cur.MaxIndex > next.MaxIndex && cur.MaxTerm >= next.MaxTerm {
		return errors.Wrapf(ErrInvariant, "Cannot swap snapshot [%v] with current [%v].  It is older", next, cur)
	}

	return badgerSetActiveSnapshotId(tx, logId, next.Id)
}

func badgerGetMaxIndexAndTerm(tx *badger.Txn, logId uuid.UUID) (max int64, term int64, err error) {
	// first try to get from the log itself.
	max, err = badgerGetMaxIndex(tx, logId)
	if err != nil {
		return
	}

	if max > -1 {
		entry, ok, err := badgerGetLogEntry(tx, logId, max)
		if err != nil || !ok {
			return -1, -1, errs.Or(err, errors.Wrapf(ErrInvariant, "Missing log entry [%v]", max))
		}

		return entry.Index, entry.Term, nil
	}

	// next, try to get it from the snapshot
	snapshotId, err := badgerGetActiveSnapshotId(tx, logId)
	if err != nil {
		return
	}

	summary, ok, err := badgerGetSnapshotSummary(tx, snapshotId)
	if err != nil || !ok {
		err = errs.Or(err, errors.Wrapf(ErrInvariant, "Missing snapshot [%v]", snapshotId))
		return
	}

	max, term = summary.MaxIndex, summary.MaxTerm
	return
}

func badgerInitLog(tx *badger.Txn, logId uuid.UUID, snapshotId uuid.UUID) error {
	e1 := tx.Set(logPrefix.UUID(logId), []byte{})
	e2 := badgerSetMinIndex(tx, logId, -1)
	e3 := badgerSetMaxIndex(tx, logId, -1)
	e4 := badgerSetActiveSnapshotId(tx, logId, snapshotId)
	return errs.Or(e1, e2, e3, e4)
}

func badgerGetLogEntry(tx *badger.Txn, logId uuid.UUID, idx int64) (ret Entry, ok bool, err error) {
	item, err := tx.Get(logEntryPrefix.UUID(logId).Int64(idx))
	if err != nil {
		if err == ErrKeyNotFound {
			err = nil
		}
		return
	}

	err = item.Value(func(val []byte) (err error) {
		ok, err = true, enc.Json.DecodeBinary(val, &ret)
		return err
	})
	return
}

func badgerDelLogEntry(tx *badger.Txn, logId uuid.UUID, idx int64) error {
	return tx.Delete(logEntryPrefix.UUID(logId).Int64(idx))
}

func badgerPutLogEntry(tx *badger.Txn, logId uuid.UUID, e Entry) error {
	bytes, err := enc.Encode(enc.Json, e)
	if err != nil {
		return err
	}
	return tx.Set(logEntryPrefix.UUID(logId).Int64(e.Index), bytes)
}

// Inserts the log entries into the log.  The entry must represent a contiguous
// set, but can be any range within the log.
func badgerPutLogEntries(tx *badger.Txn, logId uuid.UUID, batch []Entry) error {
	if len(batch) == 0 {
		return nil
	}

	last := int64(-1)
	for _, cur := range batch {
		if last != -1 && cur.Index != last+1 {
			return errors.Wrap(ErrInvariant, "Log entries must be contiguous")
		}

		if err := badgerPutLogEntry(tx, logId, cur); err != nil {
			return errors.Wrapf(err, "Error writing entry [%v] for log [%v]", cur.Index, logId)
		}

		last = cur.Index
	}

	min, err := badgerGetMinIndex(tx, logId)
	if err != nil {
		return errors.Wrapf(err, "Unable to retrieve max index for [%v]", logId)
	}

	first := batch[0].Index
	if min < 0 || first < min { // second case shouldn't be possible, but covering for it
		if err := badgerSetMinIndex(tx, logId, first); err != nil {
			return errors.Wrapf(err, "Unable to set min index for [%v]", logId)
		}
	}

	max, err := badgerGetMaxIndex(tx, logId)
	if err != nil {
		return errors.Wrapf(err, "Unable to retrieve max index for [%v]", logId)
	}

	if max < 0 || last > max {
		if err := badgerSetMaxIndex(tx, logId, last); err != nil {
			return errors.Wrapf(err, "Unable to set max index for [%v]", logId)
		}
	}
	return nil
}

func badgerAppendLogEntry(tx *badger.Txn, logId uuid.UUID, e []byte, term int64, k Kind) (ret Entry, err error) {
	max, _, err := badgerGetMaxIndexAndTerm(tx, logId)
	if err != nil {
		return
	}

	ret = Entry{
		Kind:    k,
		Term:    term,
		Index:   max + 1,
		Payload: e,
	}

	err = badgerPutLogEntries(tx, logId, []Entry{ret})
	return
}

// Scans a range of the log - [start, end)
func badgerGetLogEntries(tx *badger.Txn, logId uuid.UUID, start, end int64) (ret []Entry, err error) {
	if start < 0 {
		err = errors.Wrapf(ErrOutOfBounds, "Invalid scan start [%v]", start)
		return
	}

	ret = make([]Entry, 0, end-start)
	for cur := start; cur < end; cur++ {
		entry, ok, err := badgerGetLogEntry(tx, logId, cur)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.Wrapf(ErrNoEntry, "Missing expected entry [%v] for log [%v]", cur, logId)
		}

		ret = append(ret, entry)
	}
	return
}

func badgerTrimLeftLogEntries(tx *badger.Txn, logId uuid.UUID, until int64) (err error) {
	minIdx, err := badgerGetMinIndex(tx, logId)
	if err != nil {
		return
	}

	maxIdx, err := badgerGetMaxIndex(tx, logId)
	if err != nil {
		return
	}

	if minIdx == -1 || maxIdx == -1 || until < minIdx {
		return
	}

	if maxIdx < until {
		err = errors.Errorf("Invalid max index [%v]. Greater than log max index", until)
		return
	}

	for cur := minIdx; cur < until; cur++ {
		if err = badgerDelLogEntry(tx, logId, cur); err != nil {
			return
		}
	}

	newMin := until + 1
	newMax := maxIdx
	if until == maxIdx {
		newMin = -1
		newMax = -1
	}

	if err = badgerSetMinIndex(tx, logId, newMin); err != nil {
		err = errors.Wrapf(err, "Error setting min index [%v]", newMin)
		return
	}

	if err = badgerSetMaxIndex(tx, logId, newMax); err != nil {
		err = errors.Wrapf(err, "Error setting max index [%v]", newMax)
		return
	}
	return
}

func badgerLogExists(tx *badger.Txn, id uuid.UUID) (ok bool, err error) {
	if _, err = tx.Get(logPrefix.UUID(id)); err != nil {
		if err == ErrKeyNotFound {
			err = nil
			return
		}
	}
	ok, err = true, nil
	return
}
