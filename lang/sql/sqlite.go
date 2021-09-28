package sql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkg/errors"

	_ "github.com/mattn/go-sqlite3"
)

var (
	ErrUnsupportedType       = errors.New("Sql:UnsupportedType")
	ErrUnsupportedConstraint = errors.New("Sql:UnsupportedConstraint")
	ErrSqliteUnique          = errors.New("UNIQUE constraint failed")
	ErrUniqueConstraint      = errors.New("Sql:UniqueConstraint")
)

type SqlLiteDialect struct {
}

func (u *SqlLiteDialect) ConvertError(in error) (err error) {
	err = in
	if errors.Is(err, ErrSqliteUnique) {
		err = ErrUniqueConstraint
	}
	return
}

func (u *SqlLiteDialect) SqlType(t DataType) (ret string, err error) {
	switch t {
	default:
		ret = t.String()
	case Bool:
		ret = "bool"
	case Bytes:
		ret = "bytea"
	case String:
		ret = "text"
	case Integer:
		ret = "integer"
	case Time:
		ret = "timestamp"
	case UUID:
		ret = "char(36)"
	}
	return
}

func (u *SqlLiteDialect) SqlConstraint(c Constraint) (ret string, err error) {
	switch c.Type {
	default:
		err = errors.Wrapf(ErrUnsupportedConstraint, "Unsupported: [%v]", c.Type)
	case NotNull.Type:
		ret = "not null"
	case Unique.Type:
		ret = "unique"
	}
	return
}

func (u *SqlLiteDialect) SqlColumn(c Column) (sql string, err error) {
	typ, err := u.SqlType(c.Type)
	if err != nil {
		return
	}

	consts := []string{}
	for _, c := range c.Constraints {
		cons, err := u.SqlConstraint(c)
		if err != nil {
			return "", err
		}

		consts = append(consts, cons)
	}

	sql = strings.Join(append([]string{c.Name, typ}, consts...), " ")
	return
}

func (u *SqlLiteDialect) CreateTableStmt(table Table) (sql string, err error) {
	cols := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		sql, err := u.SqlColumn(col)
		if err != nil {
			return "", err
		}

		cols = append(cols, sql)
	}

	sql = fmt.Sprintf("create table if not exists %v(%v)", table.Name, strings.Join(cols, ","))
	return
}

func (u *SqlLiteDialect) CreateIndexStmt(table string, idx Index) (sql string, err error) {
	// fmt.Println(fmt.Sprintf("Creating index on [%v] named [%v] with cols %v", table, idx.Name, idx.Columns))
	if idx.Unique {
		sql = fmt.Sprintf("create unique index if not exists i%v on %v (%v)", idx.Name, table, strings.Join(idx.Columns, ","))
	} else {
		sql = fmt.Sprintf("create index if not exists i%v on %v (%v)", idx.Name, table, strings.Join(idx.Columns, ","))
	}
	return
}

func (u *SqlLiteDialect) AddColumnStmt(table string, c Column) (sql string, err error) {
	col, err := u.SqlColumn(c)
	if err != nil {
	}

	sql = fmt.Sprintf("alter table %v add column %v", table, col)
	return
}

func (u *SqlLiteDialect) DropColumnStmt(table, col string) (sql string, err error) {
	sql = fmt.Sprintf("alter table %v drop column if exists %v", table, col)
	return
}

type SqlLiteDialer struct {
}

func NewSqlLiteDialer() SqlLiteDialer {
	return SqlLiteDialer{}
}

func (s SqlLiteDialer) Connect(ctx context.Context, ds string, o ...Option) (Driver, error) {
	ctx = ctx.Sub("Sqlite3")
	raw, err := sql.Open("sqlite3", ds)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to open sqllite db [%v]", ds)
	}
	raw.SetMaxOpenConns(1)
	return NewDefaultDriver(ctx, raw, &SqlLiteDialect{}), nil
}

func (s SqlLiteDialer) Embed(ctx context.Context) (Driver, error) {
	return s.Connect(ctx, ":memory:")
}
