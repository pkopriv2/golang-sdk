package raft

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	_ "net/http/pprof"

	"github.com/pkopriv2/golang-sdk/lang/concurrent"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/stretchr/testify/assert"
)

var (
	LogLevel = context.Info
)

func TestHost_Close(t *testing.T) {
	ctx := context.NewContext(os.Stdout, LogLevel)
	defer ctx.Close()

	before := runtime.NumGoroutine()

	host, err := Start(ctx, ":0", WithElectionTimeout(1*time.Second))
	if !assert.Nil(t, err) {
		return
	}

	time.Sleep(1 * time.Second)
	assert.Nil(t, host.Close())
	time.Sleep(1 * time.Second)

	runtime.GC()
	assert.True(t, before <= runtime.NumGoroutine())
}

func TestHost_Cluster_ConvergeTwoPeers(t *testing.T) {
	ctx := context.NewContext(os.Stdout, LogLevel)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 2)
	if !assert.Nil(t, err) {
		return
	}

	timer := context.NewTimer(ctx.Control(), 10*time.Second)
	defer timer.Close()

	host, err := ElectLeader(timer.Closed(), cluster)
	assert.Nil(t, err)

	err = SyncAll(timer.Closed(), cluster, func(h Host) bool {
		return h.Roster().Equals(toPeers(cluster))
	})
	if !assert.Nil(t, err) {
		return
	}

	assert.NotNil(t, host)
}

func TestHost_Cluster_ConvergeThreePeers(t *testing.T) {
	ctx := context.NewContext(os.Stdout, LogLevel)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 3)
	if !assert.Nil(t, err) {
		return
	}

	timer := context.NewTimer(ctx.Control(), 10*time.Second)
	defer timer.Close()

	host, err := ElectLeader(timer.Closed(), cluster)
	assert.Nil(t, err)
	assert.NotNil(t, host)

	err = SyncAll(timer.Closed(), cluster, func(h Host) bool {
		return h.Roster().Equals(toPeers(cluster))
	})
	assert.Nil(t, err)
}

func TestHost_Cluster_ConvergeFivePeers(t *testing.T) {
	ctx := context.NewContext(os.Stdout, LogLevel)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 5)
	if !assert.Nil(t, err) {
		return
	}

	timer := context.NewTimer(ctx.Control(), 10*time.Second)
	defer timer.Close()

	host, err := ElectLeader(timer.Closed(), cluster)
	assert.Nil(t, err)
	assert.NotNil(t, host)

	err = SyncAll(timer.Closed(), cluster, func(h Host) bool {
		return h.Roster().Equals(toPeers(cluster))
	})
	assert.Nil(t, err)
}

func TestHost_Cluster_ConvergeManyPeers(t *testing.T) {
	ctx := context.NewContext(os.Stdout, LogLevel)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 7)
	if !assert.Nil(t, err) {
		return
	}

	timer := context.NewTimer(ctx.Control(), 10*time.Second)
	defer timer.Close()

	host, err := ElectLeader(timer.Closed(), cluster)
	assert.Nil(t, err)
	assert.NotNil(t, host)

	err = SyncAll(timer.Closed(), cluster, func(h Host) bool {
		return h.Roster().Equals(toPeers(cluster))
	})
	assert.Nil(t, err)
}

func TestHost_Cluster_LeaderLeave(t *testing.T) {
	ctx := context.NewContext(os.Stdout, LogLevel)
	defer func() {
		ctx.Close()
	}()

	cluster, err := StartTestCluster(ctx, 3)
	if !assert.Nil(t, err) {
		return
	}

	timer := context.NewTimer(ctx.Control(), 60*time.Second)
	defer timer.Close()

	leader, err := ElectLeader(timer.Closed(), cluster)
	if !assert.Nil(t, err) {
		return
	}

	err = SyncAll(timer.Closed(), cluster, func(h Host) bool {
		return h.Roster().Equals(toPeers(cluster))
	})
	if !assert.Nil(t, err) {
		return
	}
	if !assert.Nil(t, leader.Close()) {
		return
	}
}

func TestHost_Cluster_Append(t *testing.T) {
	ctx := context.NewContext(os.Stdout, LogLevel)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 3)
	if !assert.Nil(t, err) {
		return
	}

	timer := context.NewTimer(ctx.Control(), 60*time.Second)
	defer timer.Close()

	leader, err := ElectLeader(timer.Closed(), cluster)
	if !assert.Nil(t, err) {
		return
	}
	assert.NotNil(t, leader)

	log, err := leader.Log()
	if err != nil {
		t.FailNow()
		return
	}

	numItems := 1024

	var last Entry
	for i := 0; i < numItems; i++ {
		before := time.Now()
		last, err = log.Append(timer.Closed(), []byte(fmt.Sprintf("%v", i)))
		if err != nil {
			t.FailNow()
			return
		}
		fmt.Println(fmt.Sprintf("Wrote [%v]. Duration: %v ms", string(last.Payload), time.Now().Sub(before)))
	}

	for _, h := range cluster {
		log, err := h.Log()
		if !assert.Nil(t, err) {
			return
		}

		buf, err := log.Listen(0, 1024)
		if !assert.Nil(t, err) {
			return
		}

		for i := 0; i < numItems; {
			select {
			case <-timer.Closed():
				buf.Close()
				log.Close()
				return
			case e := <-buf.Data():
				fmt.Println(fmt.Sprintf("%-4v: Host(%v): %v", e.Index, h.Self().Id.String()[:8], string(e.Payload)))
				if e.Kind == Std {
					i++
				}
			}
		}

		buf.Close()
		log.Close()
	}

	assert.Nil(t, SyncAll(timer.Closed(), cluster, SyncTo(last.Index)))
}

func TestHost_Cluster_Append_Concurrent(t *testing.T) {
	ctx := context.NewContext(os.Stdout, LogLevel)
	defer func() {
		ctx.Close()
	}()

	cluster, err := StartTestCluster(ctx, 3)
	if !assert.Nil(t, err) {
		return
	}

	timer := context.NewTimer(ctx.Control(), 60*time.Second)
	defer timer.Close()

	leader, err := ElectLeader(timer.Closed(), cluster)
	if !assert.Nil(t, err) {
		return
	}
	assert.NotNil(t, leader)

	numItems := 100
	numRoutines := 10

	count := concurrent.NewAtomicCounter()

	watermarks := make(chan Entry, numRoutines)
	for i := 0; i < numRoutines; i++ {
		go func(i int) {
			log, err := leader.Log()
			if err != nil {
				t.FailNow()
				return
			}
			defer log.Close()

			var last Entry
			for i := 0; i < numItems; i++ {
				before := time.Now()

				last, err = log.Append(timer.Closed(), []byte(fmt.Sprintf("%v", i)))
				if err != nil {
					t.FailNow()
					return
				}

				fmt.Println(fmt.Sprintf("Wrote [%v]. Duration: %v ms", count.Inc(), time.Now().Sub(before)))
			}

			watermarks <- last
		}(i)
	}
	for i := 0; i < numRoutines; i++ {
		select {
		case <-timer.Closed():
			t.FailNow()
			return
		case entry := <-watermarks:
			if !assert.Nil(t, SyncAll(timer.Closed(), cluster, SyncTo(entry.Index))) {
				return
			}
		}
	}
}

func TestHost_Cluster_FailedLeader(t *testing.T) {
	ctx := context.NewContext(os.Stdout, LogLevel)
	defer func() {
		ctx.Close()
	}()

	cluster, err := StartTestCluster(ctx, 3)
	if !assert.Nil(t, err) {
		return
	}

	timer := context.NewTimer(ctx.Control(), 60*time.Second)
	defer timer.Close()

	leader1, err := ElectLeader(timer.Closed(), cluster)
	if !assert.Nil(t, err) {
		return
	}

	assert.Nil(t, leader1.Kill())

	time.Sleep(5 * time.Second)

	leader2, err := ElectLeader(timer.Closed(), cluster)
	if !assert.Nil(t, err) {
		return
	}

	assert.NotEqual(t, leader1.Self(), leader2.Self())
}

//func TestHost_Cluster_Join_Busy(t *testing.T) {
//conf := context.NewConfig(map[string]interface{}{
//"bourne.log.level": int(LogLevel),
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
//// "bourne.log.level": int(LogLevel),
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

func toPeers(cluster []Host) (ret Peers) {
	for _, h := range cluster {
		ret = append(ret, h.Self())
	}
	return
}
