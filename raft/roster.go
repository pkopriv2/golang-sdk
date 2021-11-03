package raft

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/errs"
)

type roster struct {
	raw []Peer
	ver *ref
}

func newRoster(init []Peer) *roster {
	return &roster{raw: init, ver: newRef(0)}
}

func (c *roster) Wait(next int64) ([]Peer, int64, bool) {
	_, ok := c.ver.WaitExceeds(next)
	peers, ver := c.Get()
	return peers, ver, ok
}

func (c *roster) Notify() {
	c.ver.Notify()
}

func (c *roster) Set(peers []Peer) {
	c.ver.Update(func(cur int64) int64 {
		c.raw = peers
		return cur + 1
	})
}

// not taking copy as it is assumed that array is immutable
func (c *roster) Get() (peers []Peer, ver int64) {
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
	go func() {
		defer r.logger.Info("Shutting down")

		for {
			r.logger.Info("Rebuilding roster")

			peers, max, err := r.reloadRosterFromSnapshot()
			if err != nil {
				r.self.ctrl.Fail(err)
				return
			}

			r.self.Roster.Set(peers)

			appends, err := r.listenAppends(max + 1)
			if err != nil {
				r.self.ctrl.Fail(err)
				return
			}
			defer appends.Close()

			commits, err := r.listenCommits(max + 1)
			if err != nil {
				r.self.ctrl.Fail(err)
				return
			}
			defer commits.Close()

			ctrl := r.self.ctrl.Sub()
			go func() {
				defer ctrl.Close()

				l := newConfigListener(appends, ctrl)
				for {
					var config Config
					select {
					case <-ctrl.Closed():
						return
					case <-l.Ctrl().Closed():
						return
					case config = <-l.Data():
					}

					r.logger.Info("Updating roster: %v", config.Peers)
					r.self.Roster.Set(config.Peers)
				}
			}()

			go func() {
				defer ctrl.Close()

				l := newConfigListener(commits, ctrl)

				// FIXME: Must play log out to make sure we aren't re-added!
				for member := false; ; {
					var config Config
					select {
					case <-ctrl.Closed():
						return
					case <-l.Ctrl().Closed():
						return
					case config = <-l.Data():
					}

					if config.Peers.Contains(r.self.Self) {
						member = true
					}

					if member && !config.Peers.Contains(r.self.Self) {
						r.logger.Info("No longer a member of the cluster [%v]", peers)
						r.self.ctrl.Close()
						ctrl.Close()
						return
					}
				}
			}()

			select {
			case <-r.self.ctrl.Closed():
				return
			case <-ctrl.Closed():
				if cause := errs.Extract(ctrl.Failure(), ErrOutOfBounds); cause != ErrOutOfBounds {
					r.self.ctrl.Fail(err)
					return
				}
			}
		}
	}()
}

func (r *rosterManager) reloadRosterFromSnapshot() ([]Peer, int64, error) {
	snapshot, err := r.self.Log.Snapshot()
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Error getting snapshot")
	}

	return snapshot.Config().Peers, snapshot.LastIndex(), nil
}

func (r *rosterManager) listenAppends(offset int64) (Listener, error) {
	r.logger.Info("Listening to appends from offset [%v]", offset)

	l, err := r.self.Log.ListenAppends(offset, 0)
	if err != nil {
		return nil, errors.Wrapf(err, "Error registering listener at offset [%v]", offset)
	}

	return l, nil
}

func (r *rosterManager) listenCommits(offset int64) (Listener, error) {
	r.logger.Info("Listening to commits from offset [%v]", offset)

	l, err := r.self.Log.ListenCommits(offset, 0)
	if err != nil {
		return nil, errors.Wrapf(err, "Error registering listener at offset [%v]", offset)
	}

	return l, nil
}

type configListener struct {
	raw  Listener
	ctrl context.Control
	ch   chan Config
}

func newConfigListener(raw Listener, ctrl context.Control) *configListener {
	l := &configListener{raw, ctrl, make(chan Config)}
	l.start()
	return l
}

func (c *configListener) Close() error {
	return c.ctrl.Close()
}

func (c *configListener) Ctrl() context.Control {
	return c.ctrl
}

func (c *configListener) Data() <-chan Config {
	return c.ch
}

func (p *configListener) start() {
	go func() {
		for {
			var next Entry
			select {
			case <-p.ctrl.Closed():
				return
			case <-p.raw.Ctrl().Closed():
				p.ctrl.Fail(errors.WithStack(p.raw.Ctrl().Failure()))
				return
			case next = <-p.raw.Data():
			}

			if next.Kind != Conf {
				continue
			}

			config, err := next.ParseConfig(enc.Json)
			if err != nil {
				p.ctrl.Fail(errors.WithStack(err))
				return
			}

			select {
			case <-p.ctrl.Closed():
				return
			case p.ch <- config:
			}
		}
	}()
}
