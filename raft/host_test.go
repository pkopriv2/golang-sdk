package raft

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	_ "net/http/pprof"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/bin"
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

	host, err := StartTestHost(ctx)
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
	defer KillTestCluster(cluster)

	timer := context.NewTimer(ctx.Control(), 10*time.Second)
	defer timer.Close()

	_, err = ElectLeader(timer.Closed(), cluster)
	if !assert.Nil(t, err) {
		return
	}

	assert.Nil(t, SyncAll(timer.Closed(), cluster, func(h Host) bool {
		return h.Roster().Equals(toPeers(cluster))
	}))
}

func TestHost_Cluster_ConvergeThreePeers(t *testing.T) {
	ctx := context.NewContext(os.Stdout, LogLevel)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 3)
	if !assert.Nil(t, err) {
		return
	}
	defer KillTestCluster(cluster)

	timer := context.NewTimer(ctx.Control(), 10*time.Second)
	defer timer.Close()

	_, err = ElectLeader(timer.Closed(), cluster)
	if !assert.Nil(t, err) {
		return
	}

	assert.Nil(t, SyncAll(timer.Closed(), cluster, func(h Host) bool {
		return h.Roster().Equals(toPeers(cluster))
	}))
}

func TestHost_Cluster_ConvergeFivePeers(t *testing.T) {
	ctx := context.NewContext(os.Stdout, LogLevel)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 5)
	if !assert.Nil(t, err) {
		return
	}
	defer KillTestCluster(cluster)

	timer := context.NewTimer(ctx.Control(), 10*time.Second)
	defer timer.Close()

	_, err = ElectLeader(timer.Closed(), cluster)
	if !assert.Nil(t, err) {
		return
	}

	assert.Nil(t, SyncAll(timer.Closed(), cluster, func(h Host) bool {
		return h.Roster().Equals(toPeers(cluster))
	}))
}

func TestHost_Cluster_ConvergeManyPeers(t *testing.T) {
	ctx := context.NewContext(os.Stdout, LogLevel)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 7)
	if !assert.Nil(t, err) {
		return
	}

	defer KillTestCluster(cluster)

	timer := context.NewTimer(ctx.Control(), 10*time.Second)
	defer timer.Close()

	_, err = ElectLeader(timer.Closed(), cluster)
	if !assert.Nil(t, err) {
		return
	}

	assert.Nil(t, SyncAll(timer.Closed(), cluster, func(h Host) bool {
		return h.Roster().Equals(toPeers(cluster))
	}))
}

func TestHost_Cluster_LeaderLeave(t *testing.T) {
	ctx := context.NewContext(os.Stdout, LogLevel)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 3)
	if !assert.Nil(t, err) {
		return
	}
	defer KillTestCluster(cluster)

	timer := context.NewTimer(ctx.Control(), 60*time.Second)
	defer timer.Close()

	leader1, err := ElectLeader(timer.Closed(), cluster)
	if !assert.Nil(t, err) {
		return
	}

	err = SyncAll(timer.Closed(), cluster, func(h Host) bool {
		return h.Roster().Equals(toPeers(cluster))
	})
	if !assert.Nil(t, err) {
		return
	}
	if !assert.Nil(t, leader1.Leave()) { // leaves the cluster
		return
	}

	// we need to give the followers enough time to start an election
	time.Sleep(1 * time.Second)

	leader2, err := ElectLeader(timer.Closed(), cluster)
	if !assert.Nil(t, err) {
		return
	}
	assert.NotEqual(t, leader1.Self(), leader2.Self())
}

func TestHost_Cluster_Close(t *testing.T) {
	ctx := context.NewContext(os.Stdout, LogLevel)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 5)
	if !assert.Nil(t, err) {
		return
	}

	assert.Nil(t, CloseTestCluster(cluster))
}

func TestHost_Cluster_LeaderFailed(t *testing.T) {
	ctx := context.NewContext(os.Stdout, LogLevel)
	defer func() {
		ctx.Close()
	}()

	cluster, err := StartTestCluster(ctx, 3)
	if !assert.Nil(t, err) {
		return
	}
	defer KillTestCluster(cluster)

	timer := context.NewTimer(ctx.Control(), 60*time.Second)
	defer timer.Close()

	leader1, err := ElectLeader(timer.Closed(), cluster)
	if !assert.Nil(t, err) {
		return
	}

	assert.Nil(t, leader1.Close())

	time.Sleep(1 * time.Second)

	leader2, err := ElectLeader(timer.Closed(), cluster)
	if !assert.Nil(t, err) {
		return
	}

	assert.NotEqual(t, leader1.Self(), leader2.Self())
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
		last, err = log.Append(timer.Closed(), bin.Int64(int64(i)))
		if err != nil {
			t.FailNow()
			return
		}
		fmt.Println(fmt.Sprintf("Wrote [%v]. Duration: %v", i, time.Now().Sub(before)))
	}

	// make sure the logs get to all hosts eventually
	done := make(chan error, len(cluster)-1)
	for _, h := range cluster {
		go func(h Host) {
			log, err := h.Log()
			if err != nil {
				done <- err
				return
			}

			listener, err := log.Listen(0, int64(numItems))
			if err != nil {
				done <- err
				return
			}

			for i := 0; i < numItems; {
				select {
				case <-timer.Closed():
					done <- err
					return
				case entry := <-listener.Data():
					if entry.Kind != Std {
						continue
					}

					if !assert.Equal(t, []byte(bin.Int64(int64(i))), []byte(entry.Payload)) {
						done <- errors.New("assertion failed")
						return
					}

					fmt.Println(fmt.Sprintf("Verified [%v]", i))
					i++
				}
			}

			done <- nil
			return
		}(h)
	}

	for i := 0; i < len(cluster)-1; i++ {
		select {
		case err := <-done:
			if !assert.Nil(t, err) {
				return
			}
		case <-timer.Closed():
			assert.Fail(t, "Timer closed")
			return
		}
	}

	assert.Nil(t, SyncAll(timer.Closed(), cluster, SyncTo(last.Index)))
}

func TestHost_Cluster_Append_Concurrent(t *testing.T) {
	ctx := context.NewContext(os.Stdout, LogLevel)
	defer ctx.Close()

	cluster, err := StartTestCluster(ctx, 3)
	if !assert.Nil(t, err) {
		return
	}
	defer KillTestCluster(cluster)

	timer := context.NewTimer(ctx.Control(), 60*time.Second)
	defer timer.Close()

	leader, err := ElectLeader(timer.Closed(), cluster)
	if !assert.Nil(t, err) {
		return
	}

	numItems := 1000
	numRoutines := 10

	count := concurrent.NewAtomicCounter()

	watermarks := make(chan Entry, numRoutines)
	for i := 0; i < numRoutines; i++ {
		go func(i int) {
			log, err := leader.Log()
			if err != nil {
				return
			}
			defer log.Close()

			var last Entry
			for i := 0; i < numItems; i++ {
				before := time.Now()

				last, err = log.Append(timer.Closed(), []byte(fmt.Sprintf("%v", i)))
				if err != nil {
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

func TestHost_Cluster_Join_Busy(t *testing.T) {
	ctx := context.NewContext(os.Stdout, LogLevel)
	defer ctx.Close()

	timer := context.NewTimer(ctx.Control(), 30*time.Second)

	cluster, err := StartTestCluster(ctx, 3)
	if !assert.Nil(t, err) {
		return
	}
	defer KillTestCluster(cluster)

	leader, err := ElectLeader(timer.Closed(), cluster)
	if !assert.Nil(t, err) {
		return
	}

	log, err := leader.Log()
	if !assert.Nil(t, err) {
		return
	}

	numThreads := 10
	numItemsPerThread := 1024

	count := concurrent.NewAtomicCounter()

	done := make(chan Entry, numThreads)
	for i := 0; i < numThreads; i++ {
		go func() {
			var last Entry
			for j := 0; j < numItemsPerThread; j++ {
				cur, err := log.Append(timer.Closed(), []byte(fmt.Sprintf("%v", count.Inc())))
				if err != nil {
					return
				}
				if cur.Index < last.Index {
					panic("ERROR.  OUT OF ORDER COMMITS!")
				}
				last = cur
			}
			done <- last
		}()
	}

	_, err = JoinTestHost(ctx, leader.Self().Addr)
	if !assert.Nil(t, err) {
		return
	}

	for i := 0; i < numThreads; i++ {
		select {
		case <-timer.Closed():
			assert.Fail(t, "Timer timed out")
			return
		case entry := <-done:
			assert.Nil(t, SyncAll(timer.Closed(), cluster, SyncTo(entry.Index)))
		}
	}
}

func toPeers(hosts []Host) (ret Peers) {
	ret = NewPeers([]Peer{})
	for _, h := range hosts {
		ret = ret.Add(h.Self())
	}
	return
}
