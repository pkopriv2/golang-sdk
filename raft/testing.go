package raft

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/context"
	uuid "github.com/satori/go.uuid"
)

func StartTestHost(ctx context.Context) (Host, error) {
	return Start(ctx, ":0",
		WithDialTimeout(1*time.Second),
		WithReadTimeout(1*time.Second),
		WithSendTimeout(1*time.Second),
		WithElectionTimeout(1*time.Second))
}

func JoinTestHost(ctx context.Context, peer string) (Host, error) {
	return Join(ctx, ":0", []string{peer},
		WithDialTimeout(1*time.Second),
		WithReadTimeout(1*time.Second),
		WithSendTimeout(1*time.Second),
		WithElectionTimeout(1*time.Second))
}

func StartTestCluster(ctx context.Context, size int) (peers []Host, err error) {
	if size < 1 {
		return []Host{}, nil
	}

	ctx = ctx.Sub("Cluster(size=%v)", size)
	defer func() {
		if err != nil {
			ctx.Control().Close()
		}
	}()

	first, err := StartTestHost(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Error starting first host")
	}

	hosts := []Host{first}
	for i := 1; i < size; i++ {
		host, err := JoinTestHost(ctx, first.Self().Addr)
		if err != nil {
			return nil, errors.Wrapf(err, "Error starting [%v] host", i)
		}

		hosts = append(hosts, host)
	}

	return hosts, nil
}

func StopTestCluster(cluster []Host) error {
	//errs := []error{}
	for _, h := range cluster {
		if err := h.Close(); err != nil {
			return err
			//errs = append(errs, err)
		}
	}
	return nil

	//if len(errs) == 0 {
	//return nil
	//}

	//return fmt.Errorf("Errors stopping cluster: %v", errs)
}

func ElectLeader(cancel <-chan struct{}, cluster []Host) (Host, error) {
	var term int64 = 0
	var leader *uuid.UUID

	err := SyncMajority(cancel, cluster, func(h Host) bool {
		copyTerm := h.(*host).replica.CurrentTerm()
		if copyTerm.Epoch > term {
			term = copyTerm.Epoch
		}

		if copyTerm.Epoch == term && copyTerm.LeaderId != nil {
			leader = copyTerm.LeaderId
		}

		return leader != nil && copyTerm.LeaderId == leader && copyTerm.Epoch == term
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

func SyncMajority(cancel <-chan struct{}, cluster []Host, fn func(h Host) bool) error {
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

func SyncAll(cancel <-chan struct{}, cluster []Host, fn func(h Host) bool) error {
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

func SyncTo(index int64) func(p Host) bool {
	return func(p Host) bool {
		log, err := p.Log()
		if err != nil {
			return false
		}
		return log.Committed() >= index
	}
}

func first(cluster []Host, fn func(h Host) bool) Host {
	for _, h := range cluster {
		if fn(h) {
			return h
		}
	}

	return nil
}
