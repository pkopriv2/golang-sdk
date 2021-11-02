package raft

import (
	"os"
	"testing"

	"github.com/pkopriv2/golang-sdk/lang/boltdb"
	"github.com/pkopriv2/golang-sdk/lang/context"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestBoltLog_CreateSnapshot_Empty(t *testing.T) {
	ctx := context.NewContext(os.Stdout, context.Debug)
	defer ctx.Close()

	db := boltdb.MustOpenTemp()
	defer boltdb.Delete(db)
	defer boltdb.Close(db)
	initBoltBuckets(db)

	s, err := createEmptyBoltSnapshot(db, Config{Peers: []Peer{}})
	if !assert.Nil(t, err) {
		return
	}

	snapshot, err := openBoltSnapshot(db, s.Id())
	if !assert.Nil(t, err) {
		return
	}
	assert.Equal(t, s.Id(), snapshot.Id())
	assert.Equal(t, s.Config(), snapshot.Config())
}

func TestBoltLog_CreateSnapshot_Config(t *testing.T) {
	ctx := context.NewContext(os.Stdout, context.Debug)
	defer ctx.Close()

	db := boltdb.MustOpenTemp()
	defer boltdb.Delete(db)
	defer boltdb.Close(db)
	initBoltBuckets(db)

	expected := Config{Peers: []Peer{Peer{uuid.NewV1(), "addr"}}}

	s, err := createEmptyBoltSnapshot(db, expected)
	if !assert.Nil(t, err) {
		return
	}

	snapshot, err := openBoltSnapshot(db, s.Id())
	if !assert.Nil(t, err) {
		return
	}
	assert.Equal(t, s.Id(), snapshot.Id())
	assert.Equal(t, expected, snapshot.Config())
}

func TestBoltLog_CreateSnapshot_Events(t *testing.T) {
	ctx := context.NewContext(os.Stdout, context.Debug)
	defer ctx.Close()

	db := boltdb.MustOpenTemp()
	defer boltdb.Delete(db)
	defer boltdb.Close(db)
	initBoltBuckets(db)

	expected := []Event{[]byte{0, 1}, []byte{0, 1}}

	s, err := createBoltSnapshot(db, 1, 1, newEventChannel(expected), Config{})
	assert.Nil(t, err)

	events, err := s.Scan(0, 2)
	assert.Nil(t, err)
	assert.Equal(t, expected, events)
}

func TestBoltLog_CreateSnapshot_MultipleWithEvents(t *testing.T) {
	ctx := context.NewContext(os.Stdout, context.Debug)
	defer ctx.Close()

	db := boltdb.MustOpenTemp()
	defer boltdb.Delete(db)
	defer boltdb.Close(db)
	initBoltBuckets(db)

	expected1 := []Event{[]byte{0, 1}, []byte{2, 3}}
	expected2 := []Event{[]byte{0, 1, 2}, []byte{3}, []byte{4, 5}}

	snapshot1, err := createBoltSnapshot(db, 1, 1, newEventChannel(expected1), Config{})
	if !assert.Nil(t, err) {
		return
	}

	snapshot2, err := createBoltSnapshot(db, 2, 2, newEventChannel(expected2), Config{})
	if !assert.Nil(t, err) {
		return
	}

	events1, err := snapshot1.Scan(0, 2)
	if !assert.Nil(t, err) {
		return
	}
	assert.Equal(t, expected1, events1)

	events2, err := snapshot2.Scan(0, 3)
	if !assert.Nil(t, err) {
		return
	}
	assert.Equal(t, expected2, events2)
}

func TestBoltLog_DeleteSnapshot(t *testing.T) {
	ctx := context.NewContext(os.Stdout, context.Debug)
	defer ctx.Close()

	db := boltdb.MustOpenTemp()
	defer boltdb.Delete(db)
	defer boltdb.Close(db)
	initBoltBuckets(db)

	events := []Event{[]byte{0, 1}, []byte{2, 3}}

	snapshot, err := createBoltSnapshot(db, 2, 1, newEventChannel(events), Config{})
	if !assert.Nil(t, err) {
		return
	}
	assert.Nil(t, snapshot.Delete())
	assert.Nil(t, snapshot.Delete()) // idempotent deletes

	_, err = snapshot.Scan(0, 2)
	assert.NotNil(t, err)
}

func TestBoltStore_New_WithConfig(t *testing.T) {
	ctx := context.NewContext(os.Stdout, context.Debug)
	defer ctx.Close()

	db := boltdb.MustOpenTemp()
	defer boltdb.Delete(db)
	defer boltdb.Close(db)

	store, err := NewBoltStore(db)
	if !assert.Nil(t, err) {
		return
	}

	id := uuid.NewV1()

	log, err := store.NewLog(id, Config{})
	if !assert.Nil(t, err) {
		return
	}

	raw := log.(*BoltLog)

	min, err := raw.Min()
	if !assert.Nil(t, err) {
		return
	}
	assert.Equal(t, int64(-1), min)
}

func TestBoltStore_Get_NoExist(t *testing.T) {
	ctx := context.NewContext(os.Stdout, context.Debug)
	defer ctx.Close()

	db := boltdb.MustOpenTemp()
	defer boltdb.Delete(db)
	defer boltdb.Close(db)

	store, err := NewBoltStore(db)
	if !assert.Nil(t, err) {
		return
	}

	log, err := store.GetLog(uuid.NewV1())
	assert.Nil(t, err)
	assert.Nil(t, log)
}

func TestBoltLog_Create_Empty(t *testing.T) {
	ctx := context.NewContext(os.Stdout, context.Debug)
	defer ctx.Close()

	db := boltdb.MustOpenTemp()
	defer boltdb.Delete(db)
	defer boltdb.Close(db)

	store, err := NewBoltStore(db)
	if !assert.Nil(t, err) {
		return
	}

	log, err := store.NewLog(uuid.NewV1(), Config{})
	if !assert.Nil(t, err) {
		return
	}

	min, _ := log.(*BoltLog).Min()
	max, _ := log.(*BoltLog).Max()

	assert.Equal(t, int64(-1), min)
	assert.Equal(t, int64(-1), max)

	lastIndex, lastTerm, err := log.LastIndexAndTerm()
	if !assert.Nil(t, err) {
		return
	}
	assert.Equal(t, int64(-1), lastIndex)
	assert.Equal(t, int64(-1), lastTerm)

	batch, _ := log.Scan(0, 0)
	assert.Empty(t, batch)

	snapshot, err := log.Snapshot()
	assert.Equal(t, int64(-1), snapshot.LastIndex())
	assert.Equal(t, int64(-1), snapshot.LastTerm())
	assert.Equal(t, int64(0), snapshot.Size())
	assert.Equal(t, Config{}, snapshot.Config())
}

func TestBoltLog_Append_Single(t *testing.T) {
	ctx := context.NewContext(os.Stdout, context.Debug)
	defer ctx.Close()

	db := boltdb.MustOpenTemp()
	defer boltdb.Delete(db)
	defer boltdb.Close(db)

	store, err := NewBoltStore(db)
	if !assert.Nil(t, err) {
		return
	}

	log, err := store.NewLog(uuid.NewV1(), Config{})
	if !assert.Nil(t, err) {
		return
	}

	exp := Entry{Index: 0, Term: 1, Payload: []byte{0}}

	item, err := log.Append(exp.Payload, exp.Term, exp.Kind)
	assert.Nil(t, err)
	assert.Equal(t, exp, item)

	batch, err := log.Scan(0, 1)
	assert.Nil(t, err)
	assert.Equal(t, []Entry{exp}, batch)
}

func TestBoltLog_Append_Multi(t *testing.T) {
	ctx := context.NewContext(os.Stdout, context.Debug)
	defer ctx.Close()

	db := boltdb.MustOpenTemp()
	defer boltdb.Delete(db)
	defer boltdb.Close(db)

	store, err := NewBoltStore(db)
	if !assert.Nil(t, err) {
		return
	}

	log, err := store.NewLog(uuid.NewV1(), Config{})
	if !assert.Nil(t, err) {
		return
	}

	exp1 := Entry{Index: 0, Term: 0, Payload: Event{0}}
	exp2 := Entry{Index: 1, Term: 1, Payload: Event{1}}

	item1, err := log.Append(exp1.Payload, exp1.Term, exp1.Kind)
	if !assert.Nil(t, err) {
		return
	}
	item2, err := log.Append(exp2.Payload, exp2.Term, exp2.Kind)
	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, exp1, item1)
	assert.Equal(t, exp2, item2)

	batch, err := log.Scan(0, 2)
	if !assert.Nil(t, err) {
		return
	}
	assert.Equal(t, []Entry{exp1, exp2}, batch)
}

//func TestBoltLog_Truncate_Empty(t *testing.T) {
//ctx := context.NewContext(context.NewEmptyConfig())
//defer ctx.Close()

//db := OpenTestLogStash(ctx)
//log, err := createBoltLog(db, uuid.NewV1(), []byte{})
//assert.Nil(t, err)
//assert.Equal(t, OutOfBoundsError, errors.Cause(log.Truncate(1)))
//}

//func TestBoltLog_Truncate_GreaterThanMax(t *testing.T) {
//ctx := context.NewContext(context.NewEmptyConfig())
//defer ctx.Close()

//db := OpenTestLogStash(ctx)
//log, err := createBoltLog(db, uuid.NewV1(), []byte{})
//assert.Nil(t, err)

//item1 := Entry{Index: 0, Term: 0, Event: Event{0}}
//item2 := Entry{Index: 1, Term: 1, Event: Event{1}}
//assert.Nil(t, log.Insert([]Entry{item1, item2}))
//assert.Equal(t, OutOfBoundsError, errors.Cause(log.Truncate(2)))
//}

//func TestBoltLog_Truncate_EqualToMax(t *testing.T) {
//ctx := context.NewContext(context.NewEmptyConfig())
//defer ctx.Close()

//db := OpenTestLogStash(ctx)
//log, err := createBoltLog(db, uuid.NewV1(), []byte{})
//assert.Nil(t, err)

//item1 := Entry{Index: 0, Term: 0, Event: Event{0}}
//item2 := Entry{Index: 1, Term: 1, Event: Event{1}}
//item3 := Entry{Index: 2, Term: 2, Event: Event{2}}
//assert.Nil(t, log.Insert([]Entry{item1, item2, item3}))
//assert.Nil(t, log.Truncate(2))

//min, _ := log.Min()
//max, _ := log.Max()

//assert.Equal(t, 0, min)
//assert.Equal(t, 1, max)

//batch, err := log.Scan(0, 100)
//assert.Nil(t, err)
//assert.Equal(t, []Entry{item1, item2}, batch)
//}

//func TestBoltLog_Truncate_EqualToMin(t *testing.T) {
//ctx := context.NewContext(context.NewEmptyConfig())
//defer ctx.Close()

//db := OpenTestLogStash(ctx)
//log, err := createBoltLog(db, uuid.NewV1(), []byte{})
//assert.Nil(t, err)

//item1 := Entry{Index: 0, Term: 0, Event: Event{0}}
//item2 := Entry{Index: 1, Term: 1, Event: Event{1}}
//item3 := Entry{Index: 2, Term: 2, Event: Event{2}}
//assert.Nil(t, log.Insert([]Entry{item1, item2, item3}))
//assert.Nil(t, log.Truncate(0))

//min, _ := log.Min()
//max, _ := log.Max()

//assert.Equal(t, -1, min)
//assert.Equal(t, -1, max)

//batch, err := log.Scan(-1, 100)
//assert.Nil(t, err)
//assert.Equal(t, []Entry{}, batch)
//}

//func TestBoltLog_Prune_EqualToMin(t *testing.T) {
//ctx := context.NewContext(context.NewEmptyConfig())
//defer ctx.Close()

//db := OpenTestLogStash(ctx)
//log, err := createBoltLog(db, uuid.NewV1(), []byte{})
//assert.Nil(t, err)

//item1 := Entry{Index: 0, Term: 0, Event: Event{0}}
//item2 := Entry{Index: 1, Term: 1, Event: Event{1}}
//item3 := Entry{Index: 2, Term: 2, Event: Event{2}}
//assert.Nil(t, log.Insert([]Entry{item1, item2, item3}))
//assert.Nil(t, log.Prune(0))

//min, _ := log.Min()
//max, _ := log.Max()

//assert.Equal(t, 1, min)
//assert.Equal(t, 2, max)

//batch, err := log.Scan(1, 100)
//assert.Nil(t, err)
//assert.Equal(t, []Entry{item2, item3}, batch)
//}

//func TestBoltLog_Prune_EqualToMax(t *testing.T) {
//ctx := context.NewContext(context.NewEmptyConfig())
//defer ctx.Close()

//db := OpenTestLogStash(ctx)
//log, err := createBoltLog(db, uuid.NewV1(), []byte{})
//assert.Nil(t, err)

//item1 := Entry{Index: 0, Term: 0, Event: Event{0}}
//item2 := Entry{Index: 1, Term: 1, Event: Event{1}}
//item3 := Entry{Index: 2, Term: 2, Event: Event{2}}
//assert.Nil(t, log.Insert([]Entry{item1, item2, item3}))
//assert.Nil(t, log.Prune(2))

//min, _ := log.Min()
//max, _ := log.Max()

//assert.Equal(t, -1, min)
//assert.Equal(t, -1, max)
//}

//func TestBoltLog_Prune_MultiBatch(t *testing.T) {
//ctx := context.NewContext(context.NewEmptyConfig())
//defer ctx.Close()

//db := OpenTestLogStash(ctx)
//log, err := createBoltLog(db, uuid.NewV1(), []byte{})
//assert.Nil(t, err)

//for i := 0; i < 1024; i++ {
//item := Entry{Index: i, Term: 0, Event: Event{0}}
//assert.Nil(t, log.Insert([]Entry{item}))
//}

//assert.Nil(t, log.Prune(1021))

//min, _ := log.Min()
//max, _ := log.Max()

//assert.Equal(t, 1022, min)
//assert.Equal(t, 1023, max)
//}

//func TestBoltLog_InstallSnapshot_Empty_LessThanPrev(t *testing.T) {
//ctx := context.NewContext(context.NewEmptyConfig())
//defer ctx.Close()

//db := OpenTestLogStash(ctx)
//log, err := createBoltLog(db, uuid.NewV1(), []byte{})
//assert.Nil(t, err)

//store, err := log.Store()
//assert.Nil(t, err)

//snapshot, err := store.NewSnapshot(-2, -2, NewEventChannel([]Event{Event{0}}), 1, []byte{})
//assert.Nil(t, err)

//assert.Equal(t, CompactionError, errors.Cause(log.Install(snapshot)))

//maxIndex, maxTerm, err := log.Last()
//assert.Nil(t, err)
//assert.Equal(t, -1, -1, maxIndex, maxTerm)
//}

//func TestBoltLog_InstallSnapshot_Empty_EqualToPrev(t *testing.T) {
//ctx := context.NewContext(context.NewEmptyConfig())
//defer ctx.Close()

//db := OpenTestLogStash(ctx)
//log, err := createBoltLog(db, uuid.NewV1(), []byte{})
//assert.Nil(t, err)

//store, err := log.Store()
//assert.Nil(t, err)

//snapshot, err := store.NewSnapshot(-1, -1, NewEventChannel([]Event{Event{0}}), 1, []byte{})
//assert.Nil(t, err)
//assert.Nil(t, log.Install(snapshot))

//maxIndex, maxTerm, err := log.Last()
//assert.Nil(t, err)
//assert.Equal(t, -1, -1, maxIndex, maxTerm)
//}

//func TestBoltLog_InstallSnapshot_EqualToMin(t *testing.T) {
//ctx := context.NewContext(context.NewEmptyConfig())
//defer ctx.Close()

//db := OpenTestLogStash(ctx)
//log, err := createBoltLog(db, uuid.NewV1(), []byte{})
//assert.Nil(t, err)

//store, err := log.Store()
//assert.Nil(t, err)

//item1 := Entry{Index: 0, Term: 0, Event: Event{0}}
//item2 := Entry{Index: 1, Term: 1, Event: Event{1}}
//item3 := Entry{Index: 2, Term: 2, Event: Event{2}}
//assert.Nil(t, log.Insert([]Entry{item1, item2, item3}))

//snapshot, err := store.NewSnapshot(0, 0, NewEventChannel([]Event{Event{0}}), 1, []byte{})
//assert.Nil(t, err)

//assert.Nil(t, log.Install(snapshot))
//assert.Nil(t, err)

//batch, err := log.Scan(1, 100)
//assert.Nil(t, err)
//assert.Equal(t, []Entry{item2, item3}, batch)

//snapshot, err = store.NewSnapshot(1, 1, NewEventChannel([]Event{Event{0}}), 1, []byte{})
//assert.Nil(t, err)
//assert.Nil(t, log.Install(snapshot))

//batch, err = log.Scan(2, 100)
//assert.Nil(t, err)
//assert.Equal(t, []Entry{item3}, batch)

//maxIndex, maxTerm, err := log.Last()
//assert.Nil(t, err)
//assert.Equal(t, 2, 2, maxIndex, maxTerm)
//}

//func TestBoltLog_InstallSnapshot_Middle(t *testing.T) {
//ctx := context.NewContext(context.NewEmptyConfig())
//defer ctx.Close()

//db := OpenTestLogStash(ctx)
//log, err := createBoltLog(db, uuid.NewV1(), []byte{})
//assert.Nil(t, err)

//store, err := log.Store()
//assert.Nil(t, err)

//item1 := Entry{Index: 0, Term: 0, Event: Event{0}}
//item2 := Entry{Index: 1, Term: 1, Event: Event{1}}
//item3 := Entry{Index: 2, Term: 2, Event: Event{2}}
//assert.Nil(t, log.Insert([]Entry{item1, item2, item3}))

//snapshot, err := store.NewSnapshot(1, 1, NewEventChannel([]Event{Event{0}}), 1, []byte{})
//assert.Nil(t, err)

//assert.Nil(t, log.Install(snapshot))

//batch, err := log.Scan(2, 100)
//assert.Nil(t, err)
//assert.Equal(t, []Entry{item3}, batch)
//}

//func TestBoltLog_InstallSnapshot_EqualToMax(t *testing.T) {
//ctx := context.NewContext(context.NewEmptyConfig())
//defer ctx.Close()

//db := OpenTestLogStash(ctx)
//log, err := createBoltLog(db, uuid.NewV1(), []byte{})
//assert.Nil(t, err)

//store, err := log.Store()
//assert.Nil(t, err)

//item1 := Entry{Index: 0, Term: 0, Event: Event{0}}
//item2 := Entry{Index: 1, Term: 1, Event: Event{1}}
//item3 := Entry{Index: 2, Term: 2, Event: Event{2}}
//assert.Nil(t, log.Insert([]Entry{item1, item2, item3}))

//snapshot, err := store.NewSnapshot(2, 2, NewEventChannel([]Event{Event{0}}), 1, []byte{})
//assert.Nil(t, err)

//assert.Nil(t, log.Install(snapshot))

//index, term, err := log.Last()
//assert.Nil(t, err)
//assert.Equal(t, index, 2)
//assert.Equal(t, term, 2)
//}

//func TestBoltLog_InstallSnapshot_GreaterThanMax(t *testing.T) {
//ctx := context.NewContext(context.NewEmptyConfig())
//defer ctx.Close()

//db := OpenTestLogStash(ctx)
//log, err := createBoltLog(db, uuid.NewV1(), []byte{})
//assert.Nil(t, err)

//store, err := log.Store()
//assert.Nil(t, err)

//item1 := Entry{Index: 0, Term: 0, Event: Event{0}}
//item2 := Entry{Index: 1, Term: 1, Event: Event{1}}
//item3 := Entry{Index: 2, Term: 2, Event: Event{2}}
//assert.Nil(t, log.Insert([]Entry{item1, item2, item3}))

//snapshot, err := store.NewSnapshot(5, 5, NewEventChannel([]Event{Event{0}}), 1, []byte{})
//assert.Nil(t, err)
//assert.Nil(t, log.Install(snapshot))

//index, term, err := log.Last()
//assert.Nil(t, err)
//assert.Equal(t, index, 5)
//assert.Equal(t, term, 5)
//}
