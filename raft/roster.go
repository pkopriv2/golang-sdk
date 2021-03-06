package raft

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/errs"
)

// A roster implements thread-safe access to the live cluster roster.
type roster struct {
	raw Peers
	ver *ref
}

func newRoster(init Peers) *roster {
	return &roster{raw: init, ver: newRef(0)}
}

func (c *roster) Wait(next int64) (Peers, int64, bool) {
	_, ok := c.ver.WaitExceeds(next)
	peers, ver := c.Get()
	return peers, ver, ok
}

func (c *roster) Notify() {
	c.ver.Notify()
}

func (c *roster) Set(peers Peers) {
	c.ver.Update(func(cur int64) int64 {
		c.raw = peers
		return cur + 1
	})
}

// not taking copy as it is assumed that array is immutable
func (c *roster) Get() (peers Peers, ver int64) {
	c.ver.Update(func(cur int64) int64 {
		peers, ver = c.raw, cur
		return cur
	})
	return
}

func (c *roster) Close() {
	c.ver.Close()
}

// FIXME: Need a better way to manage the roster!  Currently scans entire log.  Could
// scan backwards until the first config entry is found, then listen forward.
type rosterManager struct {
	logger context.Logger
	self   *replica
}

func listenRosterChanges(r *replica) {
	m := &rosterManager{r.Ctx.Logger().Fmt("RosterManager"), r}
	m.start()
}

func (r *rosterManager) start() {
	peers, maxIdx, err := r.reloadLatestConfig()
	if err != nil {
		r.self.ctrl.Fail(errors.Wrap(err, "Unable to load config"))
		return
	}

	r.logger.Info("Setting roster: %v", peers)
	r.self.Roster.Set(peers)
	go func() {
		ctrl := r.self.ctrl.Sub()
		defer r.logger.Info("Shutting down")

	Outer:
		for {
			r.logger.Info("Rebuilding roster")

			l, err := r.self.Log.ListenCommits(maxIdx+1, 0)
			if err != nil {
				r.self.ctrl.Fail(err)
				return
			}
			defer l.Close()

			for member := false; ; {
				var entry Entry
				select {
				case <-ctrl.Closed():
					return
				case <-l.Ctrl().Closed():
					if errs.Is(l.Ctrl().Failure(), ErrOutOfBounds) {
						continue Outer
					}
					return
				case entry = <-l.Data():
				}

				// Only worry about config changes
				if entry.Kind != Conf {
					continue
				}

				config, err := entry.ParseConfig(enc.Json)
				if err != nil {
					r.self.ctrl.Fail(errors.Wrapf(err, "Unable to parse config at index [%v]", entry.Index))
					return
				}

				r.logger.Info("Detected roster change: %v", config.Peers.Flatten())
				if config.Peers.Contains(r.self.Self) {
					member = true
				}

				// update the roster.
				r.self.Roster.Set(config.Peers)
				if member && !config.Peers.Contains(r.self.Self) {
					r.logger.Info("No longer a member of the cluster")
					r.self.ctrl.Close()
					ctrl.Close()
					return
				}
			}
		}
	}()
}

func (r *rosterManager) reloadLatestConfig() (ret Peers, maxIdx int64, err error) {
	snapshot, err := r.self.Log.Snapshot()
	if err != nil {
		return
	}
	ret = snapshot.Config().Peers

	maxIdx, _, err = r.self.Log.LastIndexAndTerm() // might return minIdx
	if err != nil {
		return
	}

	minIdx := snapshot.LastIndex()
	if maxIdx <= minIdx {
		return
	}

	if minIdx < 0 {
		minIdx = 0
	}

	// Start reading the log backwards until we find an entry
	for end := maxIdx + 1; end > minIdx; {
		beg := max(end-256, minIdx)

		batch, e := r.self.Log.Scan(beg, end)
		if e != nil {
			err = e
			return
		}

		for i := len(batch) - 1; i > 0; i-- {
			if batch[i].Kind != Conf {
				continue
			}

			conf, e := batch[i].ParseConfig(enc.Json)
			if e != nil {
				err = e
				return
			}

			ret = conf.Peers
			return
		}

		end = beg
	}

	return
}
