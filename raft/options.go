package raft

import (
	"time"
)

const (
	DefaultStoragePath = "/var/raft/raft.db"
)

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

func buildOptions(fns ...Option) (ret Options) {
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
