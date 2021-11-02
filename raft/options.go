package raft

import (
	"path"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/net"
	uuid "github.com/satori/go.uuid"
	"github.com/spf13/afero"
)

const (
	DefaultStoragePath = "/var/raft/raft.db"
)

type Option func(*Options)

type Options struct {
	StoragePath     string
	StorageDelete   bool
	Network         net.Network
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	SendTimeout     time.Duration
	ElectionTimeout time.Duration
	Workers         int
	MaxConns        int
	Encoder         enc.EncoderDecoder
	LogStorage      func() (LogStore, error)
	TermStorage     func() (*TermStore, error)
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
		StoragePath:     DefaultStoragePath,
		StorageDelete:   false,
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

func WithTmpStorage() Option {
	return func(o *Options) {
		o.StorageDelete = true
		o.StoragePath = path.Join(
			afero.GetTempDir(afero.NewOsFs(),
				path.Join("raft", uuid.NewV1().String())), "raft.db")
	}
}

func WithStoragePath(path string) Option {
	return func(o *Options) {
		o.StoragePath = path
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

func WithNumWorkers(num int) Option {
	return func(o *Options) {
		o.Workers = num
	}
}

func WithBoltLogStore(db *bolt.DB) Option {
	return func(o *Options) {
		o.LogStorage = func() (LogStore, error) {
			return NewBoltStore(db)
		}
	}
}

func WithBoltTermStore(db *bolt.DB) Option {
	return func(o *Options) {
		o.TermStorage = func() (*TermStore, error) {
			return openTermStore(db)
		}
	}
}
