package raft

import (
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/boltdb"
	"github.com/pkopriv2/golang-sdk/lang/context"
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
	LogStorage      func(context.Context) (LogStore, error)
	TermStorage     func(context.Context) (*termStore, error)
}

func (o Options) Update(fns ...func(*Options)) (ret Options) {
	ret = o
	for _, fn := range fns {
		fn(&ret)
	}
	return
}

func WithBoltStore(path string) func(ctx context.Context) (LogStore, error) {
	return func(ctx context.Context) (ret LogStore, err error) {
		db, err := boltdb.Open(ctx, path)
		if err != nil {
			err = errors.Wrapf(err, "Unable to open db [%v]", path)
			return
		}

		ret, err = NewBoltStore(db)
		return
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
		LogStorage:      WithBoltStore("~/.raft/data.db"),
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
