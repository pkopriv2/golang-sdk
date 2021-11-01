package raft

import (
	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/bin"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	uuid "github.com/satori/go.uuid"
)

// Bolt implementation of raft log store.
var (
	logBucket            = []byte("raft.log")
	logEntryBucket       = []byte("raft.log.entry")        // holds actual log entries. (key = /<logId>/<index>)
	logMinBucket         = []byte("raft.log.min")          // holds the beginning index of the log. (key = /<logId>)
	logMaxBucket         = []byte("raft.log.max")          // holds the ending index of the log. (key = /<logId>)
	logSnapshotBucket    = []byte("raft.log.snapshot")     // holds the uuid of the active snapshot. (key = /<logId>)
	snapshotsBucket      = []byte("raft.snapshots")        // holds the snapshot summary (key = /<snapshotId>)
	snapshotEventsBucket = []byte("raft.snapshots.events") // holds the snapshot events (key = /<snapshotId>)
)

// Ensures all the buckets are created.
func initBoltBuckets(db *bolt.DB) (err error) {
	return db.Update(func(tx *bolt.Tx) error {
		_, e1 := tx.CreateBucketIfNotExists(logBucket)
		_, e2 := tx.CreateBucketIfNotExists(logEntryBucket)
		_, e3 := tx.CreateBucketIfNotExists(logMinBucket)
		_, e4 := tx.CreateBucketIfNotExists(logMaxBucket)
		_, e5 := tx.CreateBucketIfNotExists(logSnapshotBucket)
		_, e6 := tx.CreateBucketIfNotExists(snapshotsBucket)
		_, e7 := tx.CreateBucketIfNotExists(snapshotEventsBucket)
		return errs.Or(e1, e2, e3, e4, e5, e6, e7)
	})
}

type boltSnapshotSummary struct {
	Id       uuid.UUID `json:"id"`
	MaxIndex int64     `json:"max_index"`
	MaxTerm  int64     `json:"max_term"`
	Size     int64     `json:"size"`
	Config   Config    `json:"config"`
}

func (b boltSnapshotSummary) Encode(enc enc.Encoder) (ret []byte, err error) {
	err = enc.EncodeBinary(b, &ret)
	return
}

func getSnapshotSummary(tx *bolt.Tx, snapshotId uuid.UUID) (ret boltSnapshotSummary, ok bool, err error) {
	raw := tx.Bucket(snapshotsBucket).Get(bin.UUID(snapshotId))
	if raw == nil {
		return
	}

	ok, err = true, enc.Json.DecodeBinary(raw, &ret)
	return
}

func putSnapshotSummary(tx *bolt.Tx, summary boltSnapshotSummary) (err error) {
	var bytes []byte
	if err = enc.Json.EncodeBinary(summary, &bytes); err != nil {
		return
	}

	err = tx.Bucket(snapshotsBucket).Put(bin.UUID(summary.Id), bytes)
	return
}

func deleteSnapshotSummary(tx *bolt.Tx, snapshotId uuid.UUID) error {
	return tx.Bucket(snapshotsBucket).Delete(bin.UUID(snapshotId))
}

func getSnapshotEvent(tx *bolt.Tx, snapshotId uuid.UUID, idx int64) (ret Event, ok bool, err error) {
	ret = tx.Bucket(snapshotEventsBucket).Get(bin.UUID(snapshotId).Int64(idx))
	ok = ret != nil
	return
}

func putSnapshotEvent(tx *bolt.Tx, snapshotId uuid.UUID, idx int64, e Event) error {
	return tx.Bucket(snapshotEventsBucket).Put(bin.UUID(snapshotId).Int64(idx), e)
}

// Stores the snapshot event stream into the db.  This implementation breaks the work
// into chunks to prevent blocking of concurrent reads/writes
func putSnapshotEvents(db *bolt.DB, snapshotId uuid.UUID, ch <-chan Event) (num int64, err error) {
	num = 0
	for {
		chunk := make([]Event, 0, 1024) // TODO: could implement using reusable buffer
		for i := 0; i < 1024; i++ {
			e, ok := <-ch
			if !ok {
				break
			}

			chunk = append(chunk, e)
		}

		if len(chunk) == 0 {
			return
		}

		err = db.Update(func(tx *bolt.Tx) (err error) {
			for j, e := range chunk {
				if err := putSnapshotEvent(tx, snapshotId, num+int64(j), e); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return
		}

		num += int64(len(chunk))
	}
}

// Scans a range of the snapshot - inclusive of start and end.
func getSnapshotEvents(tx *bolt.Tx, snapshotId uuid.UUID, start, end int64) (ret []Event, err error) {
	ret = make([]Event, 0, end-start+1)
	for cur := start; cur <= end; cur++ {
		e, ok, err := getSnapshotEvent(tx, snapshotId, cur)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to get snapshot event [%v] for snapshot [%v]", cur, snapshotId)
		}
		if !ok {
			return nil, errors.Wrapf(ErrInvariant, "Missing expected entry [%v] for snapshot [%v]", cur, snapshotId)
		}
		ret = append(ret, e)
	}
	return
}

// Deletes the given snapshot's event stream.  This implementation breaks the work
// into chunks to prevent blocking of concurrent reads/writes
func deleteSnapshotEvents(db *bolt.DB, snapshotId uuid.UUID) (err error) {
	prefix := bin.UUID(snapshotId)

	for contd := true; contd; {
		err = db.Update(func(tx *bolt.Tx) error {
			events := tx.Bucket(snapshotEventsBucket)
			cursor := events.Cursor()

			dead := make([][]byte, 0, 1024)
			k, _ := cursor.Seek(prefix.Int64(0))
			for i := 0; i < 1024; i++ {
				if k == nil || !prefix.ParentOf(k) {
					contd = false
					break
				}

				dead = append(dead, k)
				k, _ = cursor.Next()
			}

			for _, i := range dead {
				if err := events.Delete(i); err != nil {
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

func getMaxIndex(tx *bolt.Tx, logId uuid.UUID) (int64, error) {
	raw := tx.Bucket(logMaxBucket).Get(bin.UUID(logId))
	if raw == nil {
		return -1, nil
	}
	return bin.ParseInt64(raw)
}

func setMaxIndex(tx *bolt.Tx, logId uuid.UUID, max int64) error {
	return tx.Bucket(logMaxBucket).Put(bin.UUID(logId), bin.Int64(max))
}

func getMinIndex(tx *bolt.Tx, logId uuid.UUID) (int64, error) {
	raw := tx.Bucket(logMaxBucket).Get(bin.UUID(logId))
	if raw == nil {
		return -1, nil
	}
	return bin.ParseInt64(raw)
}

func setMinIndex(tx *bolt.Tx, logId uuid.UUID, min int64) error {
	return tx.Bucket(logMaxBucket).Put(bin.UUID(logId), bin.Int64(min))
}

func getActiveSnapshot(tx *bolt.Tx, logId uuid.UUID) (ret uuid.UUID, err error) {
	raw := tx.Bucket(logSnapshotBucket).Get(bin.UUID(logId))
	if raw != nil {
		err = errors.Errorf("Missing snapshot id for log [%v]", logId)
		return
	}

	ret, err = uuid.FromBytes(raw)
	return
}

func setActiveSnapshot(tx *bolt.Tx, logId, snapshotId uuid.UUID) error {
	return tx.Bucket(logSnapshotBucket).Put(bin.UUID(logId), bin.UUID(snapshotId))
}

func swapActiveSnapshot(tx *bolt.Tx, logId uuid.UUID, prev, next boltSnapshotSummary) error {
	curId, e := getActiveSnapshot(tx, logId)
	if e != nil {
		return e
	}

	if curId != prev.Id {
		return errors.Wrapf(ErrCompaction, "Cannot swap snapshot [%v] with current [%v].  Current is no longer active.", next, prev)
	}

	cur, ok, err := getSnapshotSummary(tx, curId)
	if err != nil || !ok {
		return errs.Or(err, errors.Wrapf(ErrInvariant, "Missing snapshot [%v]", curId))
	}

	if cur.MaxIndex > next.MaxIndex && cur.MaxTerm >= next.MaxTerm {
		return errors.Wrapf(ErrCompaction, "Cannot swap snapshot [%v] with current [%v].  It is older", next, cur)
	}

	return setActiveSnapshot(tx, logId, next.Id)
}

func getMaxIndexAndTerm(tx *bolt.Tx, logId uuid.UUID) (max int64, term int64, err error) {
	// first try to get from the log itself.
	max, err = getMaxIndex(tx, logId)
	if err != nil {
		return
	}

	if max > -1 {
		entry, ok, err := getLogEntry(tx, logId, max)
		if err != nil || !ok {
			return -1, -1, errs.Or(err, errors.Wrapf(ErrInvariant, "Missing log entry [%v]", max))
		}

		return entry.Index, entry.Term, nil
	}

	// next, try to get it from the snapshot
	snapshotId, err := getActiveSnapshot(tx, logId)
	if err != nil {
		return
	}

	summary, ok, err := getSnapshotSummary(tx, snapshotId)
	if err != nil || !ok {
		err = errs.Or(err, errors.Wrapf(ErrInvariant, "Missing snapshot [%v]", snapshotId))
		return
	}

	max, term = summary.MaxIndex, summary.MaxTerm
	return
}

func getLogEntry(tx *bolt.Tx, logId uuid.UUID, idx int64) (ret Entry, ok bool, err error) {
	raw := tx.Bucket(logEntryBucket).Get(bin.UUID(logId).Int64(idx))
	if raw == nil {
		return
	}

	ok, err = true, enc.Json.DecodeBinary(raw, &ret)
	return
}

func deleteLogEntry(tx *bolt.Tx, logId uuid.UUID, idx int64) error {
	return tx.Bucket(logEntryBucket).Delete(bin.UUID(logId).Int64(idx))
}

func putLogEntry(tx *bolt.Tx, logId uuid.UUID, e Entry) error {
	var bytes []byte
	if err := enc.Json.EncodeBinary(e, &bytes); err != nil {
		return err
	}

	return tx.Bucket(logEntryBucket).Put(bin.UUID(logId).Int64(e.Index), bytes)
}

// Inserts the log entries into the log.  The entry must represent a contiguous
// set, but can be any range within the log.
func putLogEntries(tx *bolt.Tx, logId uuid.UUID, batch []Entry) error {
	if len(batch) == 0 {
		return nil
	}

	last := int64(-1)
	for _, cur := range batch {
		if last != -1 && cur.Index != last+1 {
			return errors.Wrap(ErrInvariant, "Log entries must be contiguous")
		}

		if err := putLogEntry(tx, logId, cur); err != nil {
			return errors.Wrapf(err, "Error writing entry [%v] for log [%v]", cur.Index, logId)
		}

		last = cur.Index
	}

	min, err := getMinIndex(tx, logId)
	if err != nil {
		return errors.Wrapf(err, "Unable to retrieve max index for [%v]", logId)
	}

	first := batch[0].Index
	if min < 0 || first < min { // second case shouldn't be possible, but covering for it
		if err := setMinIndex(tx, logId, first); err != nil {
			return errors.Wrapf(err, "Unable to set min index for [%v]", logId)
		}
	}

	max, err := getMaxIndex(tx, logId)
	if err != nil {
		return errors.Wrapf(err, "Unable to retrieve max index for [%v]", logId)
	}

	if max < 0 || last > max {
		if err := setMaxIndex(tx, logId, last); err != nil {
			return errors.Wrapf(err, "Unable to set max index for [%v]", logId)
		}
	}
	return nil
}

func appendLogEntry(tx *bolt.Tx, logId uuid.UUID, e Event, term int64, k Kind) (ret Entry, err error) {
	max, _, err := getMaxIndexAndTerm(tx, logId)
	if err != nil {
		return
	}

	ret = Entry{
		Kind:    k,
		Term:    term,
		Index:   max + 1,
		Payload: e,
	}

	err = putLogEntries(tx, logId, []Entry{ret})
	return
}

func getLogEntries(tx *bolt.Tx, logId uuid.UUID, start, end int64) (ret []Entry, err error) {
	ret = make([]Entry, 0, end-start+1)
	for cur := start; cur <= end; cur++ {
		entry, ok, err := getLogEntry(tx, logId, cur)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.Wrapf(ErrInvariant, "Missing expected entry [%v] for log [%v]", cur, logId)
		}

		ret = append(ret, entry)
	}
	return
}

func pruneLogEntries(tx *bolt.Tx, logId uuid.UUID, until int64) (err error) {
	minIdx, err := getMinIndex(tx, logId)
	if err != nil {
		return
	}

	maxIdx, err := getMaxIndex(tx, logId)
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
		if err = deleteLogEntry(tx, logId, cur); err != nil {
			return
		}
	}

	newMin := until + 1
	newMax := maxIdx
	if until == maxIdx {
		newMin = -1
		newMax = -1
	}

	if err = setMinIndex(tx, logId, newMin); err != nil {
		err = errors.Wrapf(err, "Error setting min index [%v]", newMin)
		return
	}

	if err = setMaxIndex(tx, logId, newMax); err != nil {
		err = errors.Wrapf(err, "Error setting max index [%v]", newMax)
		return
	}

	return
}

func checkBoltLog(tx *bolt.Tx, id uuid.UUID) bool {
	raw := tx.Bucket(logBucket).Get(bin.UUID(id))
	return raw != nil
}

func initBoltLog(tx *bolt.Tx, logId uuid.UUID, snapshotId uuid.UUID) error {
	e1 := tx.Bucket(logBucket).Put(bin.UUID(snapshotId), []byte{})
	e2 := setMinIndex(tx, logId, -1)
	e3 := setMaxIndex(tx, logId, -1)
	e4 := setActiveSnapshot(tx, logId, snapshotId)
	return errs.Or(e1, e2, e3, e4)
}

// Store impl.
type BoltStore struct {
	db *bolt.DB
}

func NewBoltStore(db *bolt.DB) (LogStore, error) {
	if err := initBoltBuckets(db); err != nil {
		return nil, err
	}

	return &BoltStore{db}, nil
}

func (s *BoltStore) Get(id uuid.UUID) (StoredLog, error) {
	log, err := openBoltLog(s.db, id)
	if err != nil || log == nil {
		return nil, err
	}

	return log, nil
}

func (s *BoltStore) New(id uuid.UUID, config Config) (StoredLog, error) {
	return createBoltLog(s.db, id, config)
}

func (s *BoltStore) NewSnapshot(lastIndex int64, lastTerm int64, ch <-chan Event, config Config) (StoredSnapshot, error) {
	return createBoltSnapshot(s.db, lastIndex, lastTerm, ch, config)
}

// Parent log abstraction
type BoltLog struct {
	db *bolt.DB
	id uuid.UUID
}

func createBoltLog(db *bolt.DB, id uuid.UUID, config Config) (log *BoltLog, err error) {
	s, err := createEmptyBoltSnapshot(db, config)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			s.Delete()
		}
	}()

	err = db.Update(func(tx *bolt.Tx) error {
		if checkBoltLog(tx, id) {
			return errors.Wrapf(ErrInvariant, "Log [%v] already exists", id)
		}

		return initBoltLog(tx, id, s.Id())
	})
	if err != nil {
		return nil, err
	}

	return &BoltLog{db, id}, nil
}

func openBoltLog(db *bolt.DB, id uuid.UUID) (log *BoltLog, err error) {
	err = db.View(func(tx *bolt.Tx) error {
		if checkBoltLog(tx, id) {
			log = &BoltLog{db, id}
		}
		return nil
	})
	return
}

func (b *BoltLog) Id() uuid.UUID {
	return b.id
}

//func (b *BoltLog) Store() (LogStore, error) {
//return &BoltStore{b.db}, nil
//}

func (b *BoltLog) Min() (m int64, e error) {
	e = b.db.View(func(tx *bolt.Tx) error {
		m, e = getMinIndex(tx, b.Id())
		return e
	})
	return
}

func (b *BoltLog) Max() (m int64, e error) {
	e = b.db.View(func(tx *bolt.Tx) error {
		m, e = getMaxIndex(tx, b.Id())
		return e
	})
	return
}

func (b *BoltLog) LastIndexAndTerm() (i int64, t int64, e error) {
	e = b.db.View(func(tx *bolt.Tx) error {
		i, t, e = getMaxIndexAndTerm(tx, b.Id())
		return e
	})
	return
}

func (b *BoltLog) Prune(until int64) error {
	return b.db.Update(func(tx *bolt.Tx) (e error) {
		return pruneLogEntries(tx, b.Id(), until)
	})
}

func (b *BoltLog) Scan(beg int64, end int64) (i []Entry, e error) {
	e = b.db.View(func(tx *bolt.Tx) error {
		i, e = getLogEntries(tx, b.Id(), beg, end)
		return e
	})
	return
}

func (b *BoltLog) Append(event Event, term int64, kind Kind) (i Entry, e error) {
	e = b.db.Update(func(tx *bolt.Tx) error {
		i, e = appendLogEntry(tx, b.Id(), event, term, kind)
		return e
	})
	return
}

func (b *BoltLog) Get(index int64) (i Entry, o bool, e error) {
	e = b.db.View(func(tx *bolt.Tx) error {
		i, o, e = getLogEntry(tx, b.Id(), index)
		return e
	})
	return
}

func (b *BoltLog) Insert(batch []Entry) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return putLogEntries(tx, b.Id(), batch)
	})
}

func (b *BoltLog) SnapshotId() (i uuid.UUID, e error) {
	e = b.db.View(func(tx *bolt.Tx) error {
		i, e = getActiveSnapshot(tx, b.Id())
		return e
	})
	return
}

func (b *BoltLog) Snapshot() (StoredSnapshot, error) {
	id, err := b.SnapshotId()
	if err != nil {
		return nil, err
	}

	return openBoltSnapshot(b.db, id)
}

func (b *BoltLog) Install(s StoredSnapshot) error {
	cur, err := b.Snapshot()
	if err != nil {
		return err
	}
	// swap it.
	err = b.db.Update(func(tx *bolt.Tx) error {
		return swapActiveSnapshot(tx, b.Id(), cur.(*BoltSnapshot).raw, s.(*BoltSnapshot).raw)
	})
	if err != nil {
		return err
	}

	// finally, truncate (safe to do concurrently)
	err = b.Prune(s.LastIndex())
	if err != nil {
		return err
	}

	// cur is useless, regardless of whether the delete succeeds
	defer cur.Delete()
	return nil
}

type BoltSnapshot struct {
	db  *bolt.DB
	raw boltSnapshotSummary
}

func createEmptyBoltSnapshot(db *bolt.DB, config Config) (*BoltSnapshot, error) {
	return createBoltSnapshot(db, -1, -1, newEventChannel([]Event{}), config)
}

// FIXME: Cleanup erroneous snapshot installations
func createBoltSnapshot(db *bolt.DB, lastIndex int64, lastTerm int64, ch <-chan Event, config Config) (ret *BoltSnapshot, err error) {
	snapshotId := uuid.NewV1()

	num, err := putSnapshotEvents(db, snapshotId, ch)
	if err != nil {
		return
	}

	summary := boltSnapshotSummary{snapshotId, lastIndex, lastTerm, num, config}
	err = db.Update(func(tx *bolt.Tx) error {
		return putSnapshotSummary(tx, summary)
	})
	if err != nil {
		return
	}

	ret = &BoltSnapshot{db, summary}
	return
}

func openBoltSnapshot(db *bolt.DB, id uuid.UUID) (ret *BoltSnapshot, err error) {
	var raw boltSnapshotSummary
	var ok bool
	err = db.View(func(tx *bolt.Tx) error {
		raw, ok, err = getSnapshotSummary(tx, id)
		return err
	})
	if !ok || err != nil {
		err = errs.Or(err, errors.Wrapf(ErrInvariant, "Missing snapshot [%v]", id))
		return
	}

	ret = &BoltSnapshot{db, raw}
	return
}

func (b *BoltSnapshot) Id() uuid.UUID {
	return b.raw.Id
}

func (b *BoltSnapshot) Size() int64 {
	return b.raw.Size
}

func (b *BoltSnapshot) Config() Config {
	return b.raw.Config
}

func (b *BoltSnapshot) LastIndex() int64 {
	return b.raw.MaxIndex
}

func (b *BoltSnapshot) LastTerm() int64 {
	return b.raw.MaxTerm
}

func (b *BoltSnapshot) Delete() error {
	// FIXME: Probably need to atomically delete a snapshot.
	err := b.db.Update(func(tx *bolt.Tx) error {
		return deleteSnapshotSummary(tx, b.raw.Id)
	})

	return errs.Or(err, deleteSnapshotEvents(b.db, b.raw.Id))
}

func (b *BoltSnapshot) Scan(start int64, end int64) (batch []Event, err error) {
	err = b.db.View(func(tx *bolt.Tx) (err error) {
		batch, err = getSnapshotEvents(tx, b.raw.Id, start, end)
		return
	})
	return
}
