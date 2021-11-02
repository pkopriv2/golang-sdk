package raft

import (
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/net"
)

type Options struct {
	Network         net.Network
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	SendTimeout     time.Duration
	ElectionTimeout time.Duration
	Workers         int
	MaxConns        int
	Encoder         enc.EncoderDecoder
	LogStorage      func() (LogStore, error)
	TermStorage     func() (*termStore, error)
}

func (o Options) Update(fns ...func(*Options)) (ret Options) {
	ret = o
	for _, fn := range fns {
		fn(&ret)
	}
	return
}

// This is wrong! Need to share boltdb instance so can't create here!
func WithBoltStore(db *bolt.DB) func() (LogStore, error) {
	return func() (LogStore, error) {
		return NewBoltStore(db)
	}
}

type Option func(*Options)

func buildOptions(fns ...Option) (ret Options) {
	ret = Options{
		Network:         net.NewTCP4Network(),
		DialTimeout:     30 * time.Second,
		ReadTimeout:     30 * time.Second,
		SendTimeout:     30 * time.Second,
		ElectionTimeout: 30 * time.Second,
		Workers:         10,
		MaxConns:        5,
		Encoder:         enc.Json,
	}
	for _, fn := range fns {
		fn(&ret)
	}
	return
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

func WithNumWorkers(num int) Option {
	return func(o *Options) {
		o.Workers = num
	}
}
