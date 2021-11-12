package raft

import (
	"time"
)

const (
	DefaultStoragePath = "/var/raft/raft.db"
)

// This contains all the options for setting up a raft cluster and also
// represents the primary API for injecting alternative storage and transport
// implementations.
//
// There really is only a single crucial timeout setting that consumers should
// be concerned with - namely the ElectionTimeout.  At runtime peers maintain
// two internal timeouts, an election timeout (as described before) and a
// heartbeat timeout .  If a follower fails to hear a heartbeat within an election
// timeout, it starts a new candidate term and a new leader will be elected.
//
// The heartbeat timeout is currently equal to 1/5 of the election timeout. This
// multipliar may be refined over time but the expectation is that the heartbeat
// timeout is less than the election timeout.

type Option func(*Options)

type Timeouts struct {
	DialTimeout time.Duration
	ReadTimeout time.Duration
	SendTimeout time.Duration
}

type Options struct {
	LogStorage      LogStorage
	PeerStorage     PeerStorage
	Transport       Transport
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	SendTimeout     time.Duration
	ElectionTimeout time.Duration
	Workers         int
	MaxConns        int
}

func (o Options) Update(fns ...func(*Options)) (ret Options) {
	ret = o
	for _, fn := range fns {
		fn(&ret)
	}
	return
}

func (o Options) Timeouts() (ret Timeouts) {
	return Timeouts{
		DialTimeout: o.DialTimeout,
		ReadTimeout: o.ReadTimeout,
		SendTimeout: o.SendTimeout,
	}
}

func buildOptions(fns []Option) (ret Options) {
	ret = Options{
		DialTimeout:     30 * time.Second,
		ReadTimeout:     30 * time.Second,
		SendTimeout:     30 * time.Second,
		ElectionTimeout: 30 * time.Second,
		Workers:         10,
		MaxConns:        5,
	}
	for _, fn := range fns {
		fn(&ret)
	}
	return
}

func WithLogStorage(store LogStorage) Option {
	return func(o *Options) {
		o.LogStorage = store
	}
}

func WithPeerStorage(store PeerStorage) Option {
	return func(o *Options) {
		o.PeerStorage = store
	}
}

func WithTransport(tx Transport) Option {
	return func(o *Options) {
		o.Transport = tx
	}
}

func WithDialTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.DialTimeout = timeout
	}
}

func WithReadTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.ReadTimeout = timeout
	}
}

func WithSendTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.SendTimeout = timeout
	}
}

func WithElectionTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.ElectionTimeout = timeout
	}
}

func WithNumWorkers(num int) Option {
	return func(o *Options) {
		o.Workers = num
	}
}
