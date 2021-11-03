package raft

import (
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/boltdb"
	"github.com/pkopriv2/golang-sdk/lang/context"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestHost_Close(t *testing.T) {
	ctx := context.NewContext(os.Stdout, context.Off)

	before := runtime.NumGoroutine()

	host, err := Start(ctx, ":0")
	if !assert.Nil(t, err) {
		return
	}

	time.Sleep(1 * time.Second)
	assert.Nil(t, host.Close())
	time.Sleep(1 * time.Second)

	runtime.GC()
	assert.True(t, before <= runtime.NumGoroutine())
}

//func TestHost_ReadSnapshot_Empty(t *testing.T) {
//ctx := context.NewEmptyContext()
//defer ctx.Close()

//host, err := StartTestHost(ctx)
//assert.Nil(t, err)

//log, err := host.Log()
//assert.Nil(t, err)

//index, stream, err := log.Snapshot()
//assert.Nil(t, err)
//assert.Equal(t, -1, index)

//select {
//case <-stream.Ctrl().Closed():
//case <-stream.Data():
//assert.Fail(t, "Should not have received item")
//}
//}

//func TestHost_SyncBarrier_Empty(t *testing.T) {
//ctx := context.NewEmptyContext()
//defer ctx.Close()

//timer := ctx.Timer(30 * time.Second)
//defer timer.Close()

//host, err := StartTestHost(ctx)
//assert.Nil(t, err)

//sync, err := host.Sync()
//assert.Nil(t, err)

//val, err := sync.Barrier(timer.Closed())
//assert.Nil(t, err)
//assert.Equal(t, -1, val)
//}

//func TestHost_ReadSnapshot(t *testing.T) {
//conf := context.NewConfig(map[string]interface{}{
//Config.BaseElectionTimeout: 500 * time.Millisecond,
//})
//ctx := context.NewContext(conf)
//defer ctx.Close()

//timer := ctx.Timer(30 * time.Second)

//host, err := StartTestHost(ctx)
//assert.Nil(t, err)

//log, err := host.Log()
//assert.Nil(t, err)
//for i := 0; i < 3; i++ {
//log.Append(timer.Closed(), stash.IntBytes(i))
//}

//assert.Nil(t, log.Compact(1, NewEventChannel([]Event{}), 0))

//index, stream, err := log.Snapshot()
//assert.Nil(t, err)
//assert.Equal(t, 1, index)

//select {
//case <-stream.Ctrl().Closed():
//case <-stream.Data():
//assert.Fail(t, "Should not received item")
//}
//}

func TestHost_Cluster_ConvergeTwoPeers(t *testing.T) {
	ctx := context.NewContext(os.Stdout, context.Info)
	defer ctx.Close()

	cluster, err := startTestCluster(ctx, 2)
	if !assert.Nil(t, err) {
		return
	}

	timer := context.NewTimer(ctx.Control(), 10*time.Second)
	defer timer.Close()

	host, err := electLeader(timer.Closed(), cluster)
	assert.Nil(t, err)

	err = syncAll(timer.Closed(), cluster, func(h Host) bool {
		return h.Roster().Equals(toPeers(cluster))
	})
	if !assert.Nil(t, err) {
		return
	}

	assert.NotNil(t, host)
}

func TestHost_Cluster_ConvergeThreePeers(t *testing.T) {
	ctx := context.NewContext(os.Stdout, context.Off)
	defer ctx.Close()

	cluster, err := startTestCluster(ctx, 3)
	if !assert.Nil(t, err) {
		return
	}

	timer := context.NewTimer(ctx.Control(), 10*time.Second)
	defer timer.Close()

	host, err := electLeader(timer.Closed(), cluster)
	assert.Nil(t, err)

	assert.NotNil(t, host)
}

//func TestHost_Cluster_ConvergeFivePeers(t *testing.T) {
//ctx := context.NewContext(os.Stdout, context.Debug)
//defer ctx.Close()

//cluster, err := startTestCluster(ctx, 5)
//if !assert.Nil(t, err) {
//return
//}

//timer := context.NewTimer(ctx.Control(), 20*time.Second)
//defer timer.Close()

//host, err := electLeader(timer.Closed(), cluster)
//assert.Nil(t, err)
//assert.NotNil(t, host)
//}

//func TestHost_Cluster_ConvergeSevenPeers(t *testing.T) {
//ctx := context.NewContext(context.NewEmptyConfig())
//defer ctx.Close()
//cluster, err := StartTestCluster(ctx, 7)
//assert.Nil(t, err)

//timer := ctx.Timer(10 * time.Second)
//defer timer.Close()

//host, err := ElectLeader(timer.Closed(), cluster)
//assert.Nil(t, err)
//assert.NotNil(t, host)
//}

//func TestHost_Cluster_Close(t *testing.T) {
//ctx := context.NewContext(context.NewEmptyConfig())

//before := runtime.NumGoroutine()
//pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)

//cluster, err := StartTestCluster(ctx, 7)
//assert.Nil(t, err)

//timer := ctx.Timer(10 * time.Second)
//defer timer.Close()

//host, err := ElectLeader(timer.Closed(), cluster)
//assert.Nil(t, err)
//assert.NotNil(t, host)

//time.Sleep(5 * time.Second)
//ctx.Close()
//time.Sleep(5 * time.Second)

//runtime.GC()
//time.Sleep(5 * time.Second)

//pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
//assert.True(t, before >= runtime.NumGoroutine())
//}

//func TestHost_Cluster_Leader_Failure(t *testing.T) {
//conf := context.NewConfig(map[string]interface{}{
//"bourne.log.level": int(context.Debug),
//})
//ctx := context.NewContext(conf)
//defer ctx.Close()

//cluster, err := StartTestCluster(ctx, 3)
//assert.Nil(t, err)

//timer := ctx.Timer(20 * time.Second)
//defer timer.Close()

//leader, err := ElectLeader(timer.Closed(), cluster)
//assert.Nil(t, err)
//assert.NotNil(t, leader)

//leader.Close()
//ctx.Logger().Info("Leader killed. Waiting for re-election")
//time.Sleep(5 * time.Second)

//after, err := ElectLeader(timer.Closed(), cluster)
//assert.Nil(t, err)

//assert.NotNil(t, after)
//assert.NotEqual(t, leader, after)
//}

//func TestHost_Cluster_Leader_Leave(t *testing.T) {
//ctx := context.NewContext(context.NewEmptyConfig())
//defer ctx.Close()

//cluster, err := StartTestCluster(ctx, 3)
//assert.Nil(t, err)

//timer := ctx.Timer(10 * time.Second)
//leader1, err := ElectLeader(timer.Closed(), cluster)
//assert.Nil(t, err)
//assert.Nil(t, leader1.Close())

//time.Sleep(5*time.Second)

//clusterAfter := Collect(cluster, func(h Host) bool {
//return h.Id() != leader1.Id()
//})

//leader2, err := ElectLeader(timer.Closed(), clusterAfter)
//assert.NotEqual(t, leader1.Id(), leader2.Id())

//roster := leader2.Roster()
//for _, p := range roster {
//assert.NotEqual(t, leader1.Id(), p.Id)
//}
//}

//func TestHost_Cluster_Leader_Append_Single(t *testing.T) {
//conf := context.NewConfig(map[string]interface{}{
//"bourne.log.level": int(context.Debug),
//})

//ctx := context.NewContext(conf)
//defer ctx.Close()

//cluster, err := StartTestCluster(ctx, 3)
//assert.Nil(t, err)

//timer := ctx.Timer(10 * time.Second)
//defer timer.Close()

//leader, err := ElectLeader(timer.Closed(), cluster)
//assert.Nil(t, err)
//assert.NotNil(t, leader)

//item, err := leader.(*host).core.Append(Event{0, 1}, Std)
//assert.Nil(t, err)

//SyncMajority(timer.Closed(), cluster, SyncTo(item.Index))
//assert.False(t, context.IsCanceled(timer.Closed()))
//}

//func TestHost_Cluster_Leader_Append_Multi(t *testing.T) {
//conf := context.NewConfig(map[string]interface{}{
//"bourne.log.level": int(context.Debug),
//})

//ctx := context.NewContext(conf)
//defer ctx.Close()

//cluster, err := StartTestCluster(ctx, 3)
//assert.Nil(t, err)

//timer := ctx.Timer(60 * time.Second)
//defer timer.Close()

//leader, err := ElectLeader(timer.Closed(), cluster)
//assert.Nil(t, err)
//assert.NotNil(t, leader)

//numThreads := 10
//numItemsPerThread := 100

//for i := 0; i < numThreads; i++ {
//go func() {
//for j := 0; j < numItemsPerThread; j++ {
//_, err := leader.(*host).core.Append(Event(stash.Int(numThreads*i+j)), Std)
//if err != nil {
//panic(err)
//}
//}
//}()
//}

//SyncMajority(timer.Closed(), cluster, SyncTo(numThreads*numItemsPerThread-1))
//assert.False(t, context.IsCanceled(timer.Closed()))
//}

//func TestHost_Cluster_Leader_Append_WithCompactions(t *testing.T) {
//conf := context.NewConfig(map[string]interface{}{
//"bourne.log.level": int(context.Debug),
//})

//ctx := context.NewContext(conf)
//defer ctx.Close()

//cluster, err := StartTestCluster(ctx, 3)
//assert.Nil(t, err)

//timer := ctx.Timer(30 * time.Second)
//defer timer.Close()

//leader, err := ElectLeader(timer.Closed(), cluster)
//assert.Nil(t, err)
//assert.NotNil(t, leader)

//numThreads := 10
//numItemsPerThread := 100

//events := make([]Event, 0, 1024)
//for i := 0; i < 1024; i++ {
//events = append(events, Event(stash.Int(1)))
//}

//go func() {
//for range time.NewTicker(100 * time.Millisecond).C {
//if context.IsClosed(ctx.Control().Closed()) {
//return
//}

//for _, p := range cluster {
//log, err := p.Log()
//if err != nil {
//panic(err)
//}
//p.Context().Logger().Info("Running compactions: %v", log.Compact(log.Committed(), NewEventChannel(events), len(events)))
//}
//}
//}()

//for i := 0; i < numThreads; i++ {
//go func() {
//for j := 0; j < numItemsPerThread; j++ {
//_, err := leader.(*host).core.Append(Event(stash.Int(numThreads*i+j)), Std)
//if err != nil {
//panic(err)
//}
//}
//}()
//}

//SyncMajority(timer.Closed(), cluster, SyncTo(numThreads*numItemsPerThread-1))
//assert.False(t, context.IsCanceled(timer.Closed()))
//}

//func TestHost_Cluster_Append_Single(t *testing.T) {
//conf := context.NewConfig(map[string]interface{}{
//"bourne.log.level": int(context.Debug),
//})

//ctx := context.NewContext(conf)
//defer ctx.Close()

//cluster, err := StartTestCluster(ctx, 3)
//assert.Nil(t, err)

//timer := ctx.Timer(30 * time.Second)
//defer timer.Close()

//leader, err := ElectLeader(timer.Closed(), cluster)
//assert.Nil(t, err)
//assert.NotNil(t, leader)

//log, err := leader.Log()
//assert.Nil(t, err)

//item, err := log.Append(nil, Event{0})
//assert.Nil(t, err)

//SyncMajority(timer.Closed(), cluster, SyncTo(item.Index))
//assert.False(t, context.IsCanceled(timer.Closed()))
//}

//func TestCluster_Append_Multi(t *testing.T) {
//conf := context.NewConfig(map[string]interface{}{
//"bourne.log.level": int(context.Debug),
//})

//ctx := context.NewContext(conf)
//defer ctx.Close()

//cluster, err := StartTestCluster(ctx, 3)
//assert.Nil(t, err)

//timer := ctx.Timer(30 * time.Second)
//defer timer.Close()

//leader, err := ElectLeader(timer.Closed(), cluster)
//assert.Nil(t, err)
//assert.NotNil(t, leader)

//log, err := leader.Log()
//assert.Nil(t, err)

//numThreads := 100
//numItemsPerThread := 100

//for i := 0; i < numThreads; i++ {
//go func() {
//for j := 0; j < numItemsPerThread; j++ {
//_, err := log.Append(nil, Event(stash.Int(numThreads*i+j)))
//if err != nil {
//panic(err)
//}
//}
//}()
//}

//SyncMajority(timer.Closed(), cluster, SyncTo(numThreads*numItemsPerThread-1))
//assert.False(t, context.IsCanceled(timer.Closed()))
//}

//func TestHost_Cluster_Barrier(t *testing.T) {
//conf := context.NewConfig(map[string]interface{}{
//"bourne.log.level": int(context.Debug),
//})

//ctx := context.NewContext(conf)
//defer ctx.Close()

//cluster, err := StartTestCluster(ctx, 3)
//assert.Nil(t, err)

//timer := ctx.Timer(30 * time.Second)
//defer timer.Close()

//leader, err := ElectLeader(timer.Closed(), cluster)
//assert.Nil(t, err)
//assert.NotNil(t, leader)

//log, err := leader.Log()
//assert.Nil(t, err)

//sync, err := leader.Sync()
//assert.Nil(t, err)

//numThreads := 10
//numItemsPerThread := 100

//max := concurrent.NewAtomicCounter()
//for i := 0; i < numThreads; i++ {
//go func() {
//for j := 0; j < numItemsPerThread; j++ {
//item, err := log.Append(nil, Event(stash.Int(numThreads*i+j)))
//if err != nil {
//panic(err)
//}

//max.Update(func(cur uint64) uint64 {
//return uint64(context.Max(int(cur), item.Index))
//})
//}
//}()
//}

//go func() {
//for {
//val, err := sync.Barrier(nil)
//assert.Nil(t, err)
//cur := max.Get()
//assert.True(t, uint64(val) <= cur)
//leader.Context().Logger().Info("Barrier(val=%v, max=%v)", val, cur)
//time.Sleep(5 * time.Millisecond)
//}
//}()

//SyncMajority(timer.Closed(), cluster, SyncTo(numThreads*numItemsPerThread-1))
//assert.False(t, context.IsCanceled(timer.Closed()))
//}

//func TestHost_Cluster_Join_Busy(t *testing.T) {
//conf := context.NewConfig(map[string]interface{}{
//"bourne.log.level": int(context.Debug),
//})
//ctx := context.NewContext(conf)
//defer ctx.Close()

//timer := ctx.Timer(300*time.Second)

//cluster, err := StartTestCluster(ctx, 5)
//assert.Nil(t, err)

//leader, err := ElectLeader(timer.Closed(), cluster)
//assert.Nil(t, err)
//assert.NotNil(t, leader)

//log, err := leader.Log()
//assert.Nil(t, err)

//numThreads := 100
//numItemsPerThread := 100

//for i := 0; i < numThreads; i++ {
//go func() {
//for j := 0; j < numItemsPerThread; j++ {
//_, err := log.Append(timer.Closed(), Event(stash.Int(numThreads*i+j)))
//if err != nil {
//panic(err)
//}
//}
//}()
//}

//SyncMajority(timer.Closed(), cluster, SyncTo(numThreads*numItemsPerThread/4))
//assert.Nil(t, err)

//joined, err := JoinTestHost(ctx, leader.Addr())
//assert.Nil(t, err)

//SyncMajority(timer.Closed(), []Host{joined}, SyncTo(numThreads*numItemsPerThread-1))
//}

//// func benchmarkCluster_Append(log Log, threads int, itemsPerThread int, t *testing.B) {
//// for i := 0; i < threads; i++ {
//// go func() {
//// for j := 0; j < itemsPerThread; j++ {
//// for k := 0; k < t.N; k++ {
//// _, err := log.Append(nil, Event(stash.Int(threads*i+j)))
//// if err != nil {
//// panic(err)
//// }
//// }
//// }
//// }()
//// }
//// }
////
//// func Benchmark_Append_3_10_10(t *testing.B) {
//// conf := context.NewConfig(map[string]interface{}{
//// "bourne.log.level": int(context.Debug),
//// })
////
//// ctx := context.NewContext(conf)
//// defer ctx.Close()
////
//// cluster, err := TestCluster(ctx, 3)
//// if err != nil {
//// t.FailNow()
//// }
////
//// leader := Converge(nil, cluster)
//// if leader == nil {
//// t.FailNow()
//// }
////
//// log, err := leader.Log()
//// if err != nil {
//// t.FailNow()
//// }
////
//// t.ResetTimer()
//// leader.(*host).logger.Info("Starting benchmark (peers=3,threads=10,items=10)")
//// benchmarkCluster_Append(log, 10, 10, t)
//// }

//func SyncTo(index int) func(p Host) bool {
//return func(p Host) bool {
//log, err := p.Log()
//if err != nil {
//return false
//}

//return log.Head() >= index && log.Committed() >= index
//}
//}

func startTestHost(ctx context.Context, db *bolt.DB) (Host, error) {
	return Start(ctx, ":0", WithBoltDB(db), WithElectionTimeout(1*time.Second))
}

func joinTestHost(ctx context.Context, db *bolt.DB, peer string) (Host, error) {
	return Join(ctx, ":0", []string{peer}, WithBoltDB(db), WithElectionTimeout(1*time.Second))
}

func startTestCluster(ctx context.Context, size int) (peers []Host, err error) {
	if size < 1 {
		return []Host{}, nil
	}

	ctx = ctx.Sub("Cluster(size=%v)", size)
	defer func() {
		if err != nil {
			ctx.Control().Close()
		}
	}()

	db, err := boltdb.OpenTemp()
	if err != nil {
		return nil, errors.Wrap(err, "Error opening bolt instance")
	}
	ctx.Control().Defer(func(error) {
		//db.Close() FIXME: NEED TO FIGURE OUT WHY THIS CAUSES SEGFAULT
	})

	first, err := startTestHost(ctx, db)
	if err != nil {
		return nil, errors.Wrap(err, "Error starting first host")
	}
	ctx.Control().Defer(func(error) {
		first.Close()
	})

	//first, err = electLeader(ctx.Control().Closed(), []Host{first})
	//if first == nil {
	//return nil, errors.Wrap(ErrNoLeader, "First member failed to become leader")
	//}

	hosts := []Host{first}
	for i := 1; i < size; i++ {
		host, err := joinTestHost(ctx, db, first.Self().Addr)
		if err != nil {
			return nil, errors.Wrapf(err, "Error starting [%v] host", i)
		}

		hosts = append(hosts, host)
		ctx.Control().Defer(func(error) {
			host.Close()
		})
	}

	return hosts, nil
}

func electLeader(cancel <-chan struct{}, cluster []Host) (Host, error) {
	var term int64 = 0
	var leader *uuid.UUID

	err := syncMajority(cancel, cluster, func(h Host) bool {
		copyTerm := h.(*host).replica.CurrentTerm()
		if copyTerm.Num > term {
			term = copyTerm.Num
		}

		if copyTerm.Num == term && copyTerm.LeaderId != nil {
			leader = copyTerm.LeaderId
		}

		return leader != nil && copyTerm.LeaderId == leader && copyTerm.Num == term
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if leader == nil {
		return nil, nil
	}

	return first(cluster, func(h Host) bool {
		return h.Self().Id == *leader
	}), nil
}

func syncMajority(cancel <-chan struct{}, cluster []Host, fn func(h Host) bool) error {
	done := make(map[uuid.UUID]struct{})
	start := time.Now()

	majority := majority(len(cluster))
	for len(done) < majority {
		for _, h := range cluster {
			if context.IsClosed(cancel) {
				return ErrCanceled
			}

			if _, ok := done[h.Self().Id]; ok {
				continue
			}

			if fn(h) {
				done[h.Self().Id] = struct{}{}
				continue
			}

			if time.Now().Sub(start) > 10*time.Second {
				h.(*host).Context().Logger().Info("Still not sync'ed")
			}
		}
		<-time.After(250 * time.Millisecond)
	}
	return nil
}

func syncAll(cancel <-chan struct{}, cluster []Host, fn func(h Host) bool) error {
	done := make(map[uuid.UUID]struct{})
	start := time.Now()

	for len(done) < len(cluster) {
		for _, h := range cluster {
			if context.IsClosed(cancel) {
				return ErrCanceled
			}

			if _, ok := done[h.Self().Id]; ok {
				continue
			}

			if fn(h) {
				done[h.Self().Id] = struct{}{}
				continue
			}

			if time.Now().Sub(start) > 10*time.Second {
				h.(*host).Context().Logger().Info("Still not synced")
			}
		}
		<-time.After(250 * time.Millisecond)
	}
	return nil
}

func toPeers(cluster []Host) (ret Peers) {
	for _, h := range cluster {
		ret = append(ret, h.Self())
	}
	return
}

func first(cluster []Host, fn func(h Host) bool) Host {
	for _, h := range cluster {
		if fn(h) {
			return h
		}
	}

	return nil
}

func index(cluster []Host, fn func(h Host) bool) int {
	for i, h := range cluster {
		if fn(h) {
			return i
		}
	}

	return -1
}

func collect(cluster []Host, fn func(h Host) bool) []Host {
	ret := make([]Host, 0, len(cluster))
	for _, h := range cluster {
		if fn(h) {
			ret = append(ret, h)
		}
	}
	return ret
}
