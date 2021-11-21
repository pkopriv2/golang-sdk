package sql

import (
	"database/sql"
	"encoding"
	"encoding/json"
	"reflect"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	uuid "github.com/satori/go.uuid"
)

type DefaultDriver struct {
	ctx     context.Context
	log     context.Logger
	db      *sql.DB
	dialect Dialect
}

func NewDefaultDriver(ctx context.Context, db *sql.DB, d Dialect) Driver {
	ctx = ctx.Sub("DefaultDriver")
	ctx.Control().Defer(func(error) {
		ctx.Logger().Debug("Shutting down db [err=%+v]", db.Close())
	})
	return &DefaultDriver{ctx, ctx.Logger(), db, d}
}

func (d *DefaultDriver) Close() error {
	return errs.Or(d.DB().Close(), d.ctx.Close())
}

func (d *DefaultDriver) DB() *sql.DB {
	return d.db
}

func (d *DefaultDriver) Begin() (*sql.Tx, error) {
	tx, err := d.DB().Begin()
	return tx, errors.Wrap(err, "Error beginning transaction")
}

func (d *DefaultDriver) Do(fn func(Tx) error) (err error) {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	start := time.Now()
	defer func() {
		if err != nil {
			d.log.Error("Executing rollback: [reason=%v]: %v", err, tx.Rollback())
		} else {
			err = tx.Commit()
		}

		d.log.Debug("Tx time [%v]", time.Now().Sub(start))
	}()

	err = errors.WithStack(fn(&DefaultTx{d.log, tx, d.dialect}))
	return
}

type DefaultTx struct {
	log     context.Logger
	tx      *sql.Tx
	dialect Dialect
}

func (b *DefaultTx) Compile(q Query) (sql string, binds []interface{}, err error) {
	sql, vars, err := q.ToSql(b.dialect)
	if err != nil {
		err = errors.Wrap(err, "Error compiling query")
		return
	}

	binds = make([]interface{}, 0, len(vars))
	for _, v := range vars {
		u, err := bindVar(reflect.ValueOf(v))
		if err != nil {
			return "", nil, err
		}

		binds = append(binds, u)
	}

	b.log.Debug("Compiled query: %v (%v)", sql, binds)
	return
}

func (t *DefaultTx) Exec(s Query) (n int64, err error) {
	query, args, err := t.Compile(s)
	if err != nil {
		err = errors.Wrap(err, "Error compiling statement")
		return
	}

	stmt, err := t.tx.Prepare(query)
	if err != nil {
		err = errors.Wrapf(err, "Error preparing statement:\n%v", query)
		return
	}

	result, err := stmt.Exec(args...)
	if err != nil {
		err = t.dialect.ConvertError(err)
		return
	}

	n, err = result.RowsAffected()
	if err != nil {
		err = errors.Wrap(err, "Error obtaining result")
		return
	}
	return
}

func (t *DefaultTx) Query(v Object, q Query) (ok bool, err error) {
	sql, args, err := t.Compile(q)
	if err != nil {
		return
	}

	stmt, err := t.tx.Prepare(sql)
	if err != nil {
		err = errors.Wrapf(err, "Error preparing statement:\n%v", sql)
		return
	}

	rows, err := stmt.Query(args...)
	if err != nil {
		err = errors.Wrapf(err, "Error executing statement:\n%v", sql)
		return
	}
	defer rows.Close()
	if !rows.Next() {
		return
	}

	cols, err := rows.Columns()
	if err != nil {
		return
	}

	fields, err := v.Fields()
	if err != nil {
		return
	}

	var ptrs []interface{}
	for _, f := range fields {
		ptrs = append(ptrs, f)
	}

	if len(cols) > len(ptrs) {
		ptrs = append(ptrs, nils(len(cols)-len(ptrs))...)
	} else {
		ptrs = ptrs[:len(cols)]
	}

	if err = rows.Scan(ptrs...); err != nil {
		return
	}

	ok = true
	return
}

func (t *DefaultTx) Scan(dest Buffer, q Query) (n int64, err error) {
	sql, args, err := t.Compile(q)
	if err != nil {
		return
	}

	stmt, err := t.tx.Prepare(sql)
	if err != nil {
		t.log.Error("Error: %+v", err)
		err = errors.Wrapf(err, "Error preparing statement:\n%v", sql)
		return
	}

	rows, err := stmt.Query(args...)
	if err != nil {
		err = errors.Wrapf(err, "Error executing statement:\n%v", sql)
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return
	}

	for n = int64(0); rows.Next(); n++ {

		fields, err := dest.Next().Fields()
		if err != nil {
			return 0, err
		}

		var ptrs []interface{}
		for _, f := range fields {
			ptrs = append(ptrs, f)
		}

		if len(cols) > len(ptrs) {
			ptrs = append(ptrs, nils(len(cols)-len(ptrs))...)
		} else {
			ptrs = ptrs[:len(cols)]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return 0, err
		}
	}
	return
}

func bindVar(val reflect.Value) (interface{}, error) {
	switch t := val.Interface().(type) {
	default:
		if val.Kind() == reflect.Slice {
			return bindSlice(val)
		}

		return t, nil
	case []byte, *[]byte:
		return t, nil
	case time.Time, *time.Time:
		return t, nil
	case uuid.UUID:
		return t.String(), nil
	case encoding.BinaryMarshaler:
		return t.MarshalBinary()
	case encoding.TextMarshaler:
		return t.MarshalText()
	case json.Marshaler:
		return t.MarshalJSON()
	}
}

type stringer interface {
	String() string
}

func bindSlice(val reflect.Value) (ret interface{}, err error) {
	if val.Kind() != reflect.Slice {
		err = errors.Wrapf(ErrInvalidType, "Invalid type [%v]", val)
		return
	}

	elemType := val.Type().Elem()
	switch elemType.Kind() {
	case reflect.Bool:
		return pq.Array(val.Interface()), nil
	case reflect.String:
		return pq.Array(val.Interface()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return pq.Array(val.Interface()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return pq.Array(val.Interface()), nil
	case reflect.Float32, reflect.Float64:
		return pq.Array(val.Interface()), nil
	}

	var s stringer
	if elemType.AssignableTo(reflect.TypeOf(&s).Elem()) {
		var arr []string
		for i, n := 0, val.Len(); i < n; i++ {
			arr = append(arr, val.Index(i).Interface().(stringer).String())
		}

		ret = pq.Array(arr)
		return
	}

	err = errors.Wrapf(ErrInvalidType, "Cannot bind [%v]", val.Type())
	return
}
