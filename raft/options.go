package raft

import (
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/net"
)

const (
	DefaultStoragePath = "/var/raft/raft.db"
)

type Option func(*Options)

type Options struct {
	BadgerDB        *badger.DB
	Network         net.Network
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	SendTimeout     time.Duration
	ElectionTimeout time.Duration
	Workers         int
	MaxConns        int
	Encoder         enc.EncoderDecoder
}

func (o Options) Update(fns ...func(*Options)) (ret Options) {
	ret = o
	for _, fn := range fns {
		fn(&ret)
	}
	return
}

func buildOptions(fns ...Option) (ret Options) {
	ret = Options{
		Network:         net.NewTCP4Network(),
		DialTimeout:     30 * time.Second,
		ReadTimeout:     30 * time.Second,
		SendTimeout:     30 * time.Second,
		ElectionTimeout: 30 * time.Second,
		Workers:         10,
		MaxConns:        5,
		Encoder:         enc.Gob,
	}
	for _, fn := range fns {
		fn(&ret)
	}
	return
}

func WithBadgerDB(db *badger.DB) Option {
	return func(o *Options) {
		o.BadgerDB = db
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
