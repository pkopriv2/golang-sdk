package sql

import (
	"database/sql"

	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkg/errors"

	_ "github.com/lib/pq"
)

type PostgresDialer struct {
}

func NewPostgresDialer() *PostgresDialer {
	return &PostgresDialer{}
}

func (s *PostgresDialer) Connect(ctx context.Context, ds string, o ...Option) (ret Driver, err error) {
	ctx = ctx.Sub("Postgres")

	raw, err := sql.Open("postgres", ds)
	if err != nil {
		err = errors.Wrapf(err, "Unable to open postgres db [%v]", ds)
		return
	}

	opts := buildOptions(o...)
	if opts.MaxOpenConns != nil {
		raw.SetMaxOpenConns(*opts.MaxOpenConns)
	}
	if opts.MaxIdleConns != nil {
		raw.SetMaxIdleConns(*opts.MaxIdleConns)
	}
	if opts.MaxConnLifetime != nil {
		raw.SetConnMaxLifetime(*opts.MaxConnLifetime)
	}
	return NewDefaultDriver(ctx, raw, &SqlLiteDialect{}), nil
}

func (s *PostgresDialer) Embed(ctx context.Context) (Driver, error) {
	panic("cannot embed a postgres db")
}
