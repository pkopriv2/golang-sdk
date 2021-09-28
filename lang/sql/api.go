package sql

import (
	"context"
	"io"
	"time"
)

// Standard configuration for connecting to sql dbs.
type Options struct {
	MaxOpenConns    *int           //
	MaxIdleConns    *int           // should be some fraction of the total number of conns
	MaxConnLifetime *time.Duration // may be necessary in certain network scenarios that don't allow long lived conns
}

func buildOptions(fns ...Option) (ret Options) {
	for _, fn := range fns {
		fn(&ret)
	}
	return
}

type Option func(*Options)

// A dialect abstracts the various ddl and dml specific behavior
type Dialect interface {

	// Attempts to convert the error into a sqlx compatible error.
	ConvertError(error) error

	// Creates the given table. Implementations must document
	// support for the various types.
	CreateTableStmt(Table) (string, error)

	// Adds index to the table. This action may block concurrent
	// reads or writes during index creation.
	CreateIndexStmt(table string, idx Index) (string, error)

	// Adds a column to the table. This action may block concurrent
	// reads or writes during column creation.
	AddColumnStmt(table string, c Column) (string, error)
}

// A database driver.  Provides threadsafe, transactional access
// to the underlying db.
type Driver interface {
	io.Closer
	Do(func(Tx) error) error
}

// A dialer is a factory for instantiating new driver instances.
type Dialer interface {
	Connect(context.Context, string, ...Option) (Driver, error)
}

// A query is anything that can be compiled into a sql string
// and a flattened list of bindings.
type Query interface {
	ToSql(Dialect) (string, []interface{}, error)
}

// A thin abstraction around the internal database/sql Tx.
type Tx interface {
	Exec(Query) (int64, error)
	Query(Object, Query) (bool, error)
	Scan(Buffer, Query) (int64, error)
}

// A simple query function interface.
type QueryFn func(Dialect) (string, []interface{}, error)

// Implements the query interface
func (q QueryFn) ToSql(d Dialect) (string, []interface{}, error) {
	return q(d)
}

var (
	Nothing Atomic = func(Tx) error { return nil }
)

// An atomic is simply a block to be executed within
// a transactionn
type Atomic func(Tx) error

func (b Atomic) Then(fn Atomic) Atomic {
	return func(tx Tx) (err error) {
		if err = b(tx); err != nil {
			return
		}

		err = fn(tx)
		return
	}
}

func (b Atomic) ThenExec(q ...Query) Atomic {
	return b.Then(Exec(q...))
}

func (b Atomic) Exec(tx Tx) error {
	return b(tx)
}

// Compiles a variadic list of queries into an atomic
func Exec(queries ...Query) Atomic {
	return func(tx Tx) (err error) {
		for _, q := range queries {
			if _, err = tx.Exec(q); err != nil {
				return
			}
		}
		return
	}
}

var CreateTx = Exec
