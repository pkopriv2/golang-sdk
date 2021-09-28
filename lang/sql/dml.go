package sql

import (
	"database/sql/driver"
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/lib/pq"
	uuid "github.com/satori/go.uuid"
)

// Columns provide some basic logic over standard sql strings.
type Columns []string

func Cols(cols ...string) Columns {
	return Columns(cols)
}

func (c Columns) As(alias string) Columns {
	ret := make([]string, 0, len(c))
	for _, col := range c {
		ret = append(ret, fmt.Sprintf("%v.%v", alias, col))
	}
	return ret
}

func (c Columns) Match(fn func(string) bool) Columns {
	ret := make([]string, 0, len(c))
	for _, col := range c {
		if fn(col) {
			ret = append(ret, col)
		}
	}
	return ret
}

func (c Columns) Only(ids ...string) Columns {
	return c.Match(func(col string) bool {
		for _, c := range ids {
			if col == c {
				return true
			}
		}
		return false
	})
}

func (c Columns) Not(not ...string) Columns {
	return c.Match(func(col string) bool {
		for _, c := range not {
			if col == c {
				return false
			}
		}
		return true
	})
}

func (c Columns) Union(o Columns) Columns {
	return append(c, o...)
}

func (c Columns) Join(sep string) string {
	return strings.Join(c, sep)
}

// adapters to squirrel selectors
type SelectBuilder struct {
	sq sq.SelectBuilder
}

func Select(cols ...string) SelectBuilder {
	return SelectBuilder{sq.Select(cols...)}
}

func SelectSchema(s Schema) SelectBuilder {
	return Select(s.Cols()...).From(s.Name)
}

func SelectSchemaAs(s Schema, as string) SelectBuilder {
	return Select(s.Cols().As(as)...).From(s.As(as))
}

func SelectIntoStruct(val interface{}) SelectBuilder {
	var cols Columns
	for _, c := range structColumns(val) {
		cols = append(cols, c.Name)
	}
	return Select(cols...)
}

func SelectIntoStructAs(val interface{}, alias string) SelectBuilder {
	var cols Columns
	for _, c := range structColumns(val) {
		cols = append(cols, c.Name)
	}
	return Select(cols.As(alias)...)
}

func (s SelectBuilder) Distinct() SelectBuilder {
	return SelectBuilder{s.sq.Distinct()}
}

func (s SelectBuilder) From(src ...string) SelectBuilder {
	return SelectBuilder{s.sq.From(strings.Join(src, ","))}
}

func (s SelectBuilder) Where(clause string, vars ...interface{}) SelectBuilder {
	return SelectBuilder{s.sq.Where(clause, vars...)}
}

func (s SelectBuilder) Wheref(format string, vars ...interface{}) SelectBuilder {
	return SelectBuilder{s.sq.Where(fmt.Sprintf(format, vars...))}
}

func (s SelectBuilder) Having(clause string, vars ...interface{}) SelectBuilder {
	return SelectBuilder{s.sq.Having(clause, vars...)}
}

func (s SelectBuilder) WhereIn(format string, values ...interface{}) SelectBuilder {
	binds := []string{}
	for range values {
		binds = append(binds, "?")
	}

	return SelectBuilder{s.sq.Where(fmt.Sprintf(format, strings.Join(binds, ",")), values...)}
}

func (s SelectBuilder) LeftJoin(table, clause string, vars ...interface{}) SelectBuilder {
	return SelectBuilder{s.sq.LeftJoin(fmt.Sprintf("%v on %v", table, clause), vars...)}
}

func (s SelectBuilder) Join(table, clause string, vars ...interface{}) SelectBuilder {
	return SelectBuilder{s.sq.Join(fmt.Sprintf("%v on %v", table, clause), vars...)}
}

func (s SelectBuilder) OrderBy(orderBys ...string) SelectBuilder {
	return SelectBuilder{s.sq.OrderBy(orderBys...)}
}

func (s SelectBuilder) GroupBy(groupBys ...string) SelectBuilder {
	return SelectBuilder{s.sq.GroupBy(groupBys...)}
}

func (s SelectBuilder) Offset(offset uint64) SelectBuilder {
	return SelectBuilder{s.sq.Offset(offset)}
}

func (s SelectBuilder) Limit(limit uint64) SelectBuilder {
	return SelectBuilder{s.sq.Limit(limit)}
}

func (s SelectBuilder) ToSql(Dialect) (sql string, binds []interface{}, err error) {
	sql, binds, err = s.sq.ToSql()
	if err != nil {
		return
	}

	sql, err = sq.Dollar.ReplacePlaceholders(sql)
	return
}

type InsertBuilder struct {
	sq sq.InsertBuilder
}

func InsertInto(into string) InsertBuilder {
	return InsertBuilder{sq.Insert(into)}
}

func (s InsertBuilder) Columns(cols ...string) InsertBuilder {
	return InsertBuilder{s.sq.Columns(cols...)}
}

func (s InsertBuilder) Values(vals ...interface{}) InsertBuilder {
	return InsertBuilder{s.sq.Values(vals...)}
}

func (s InsertBuilder) Struct(val interface{}) InsertBuilder {
	cols, vals := structColumns(val), structValues(val)

	names := []string{}
	for i := 0; i < len(cols); i++ {
		names = append(names, cols[i].Name)
	}

	return InsertBuilder{s.sq.Columns(names...).Values(vals...)}
}

func (s InsertBuilder) ToSql(Dialect) (sql string, binds []interface{}, err error) {
	sql, raw, err := s.sq.ToSql()
	if err != nil {
		return
	}

	binds = make([]interface{}, 0, len(raw))
	for _, val := range raw {
		switch t := val.(type) {
		case driver.Valuer, time.Time, *time.Time:
			binds = append(binds, val)
			continue
		case encoding.BinaryMarshaler:
			bytes, err := t.MarshalBinary()
			if err != nil {
				return "", nil, err
			}

			binds = append(binds, bytes)
			continue
		case encoding.TextMarshaler:
			bytes, err := t.MarshalText()
			if err != nil {
				return "", nil, err
			}

			binds = append(binds, string(bytes))
			continue
		case json.Marshaler:
			bytes, err := t.MarshalJSON()
			if err != nil {
				return "", nil, err
			}

			binds = append(binds, string(bytes))
			continue
		}

		if reflect.ValueOf(val).Kind() == reflect.Struct {
			var bytes []byte
			if err := enc.Json.EncodeBinary(val, &bytes); err != nil {
				return "", nil, err
			}

			binds = append(binds, string(bytes))
			continue
		}

		binds = append(binds, val)
	}

	sql, err = sq.Dollar.ReplacePlaceholders(sql)
	return
}

// *Patch* to squirrel selector to allow consistent setting
// of columns and values.
type DeleteBuilder struct {
	sq sq.DeleteBuilder
}

// This is a patch to fix the squirrel update builder,
// where it only allows a single column to be at a time
func DeleteFrom(from string) DeleteBuilder {
	return DeleteBuilder{sq.Delete(from)}
}

func (u DeleteBuilder) Where(clause string, val ...interface{}) DeleteBuilder {
	return DeleteBuilder{u.sq.Where(clause, val...)}
}

func (u DeleteBuilder) Wheref(sql string, args ...interface{}) DeleteBuilder {
	return DeleteBuilder{u.sq.Where(fmt.Sprintf(sql, args...))}
}

func (u DeleteBuilder) WhereIn(format string, values ...interface{}) DeleteBuilder {
	binds := []string{}
	for range values {
		binds = append(binds, "?")
	}

	return DeleteBuilder{u.sq.Where(fmt.Sprintf(format, strings.Join(binds, ",")), values...)}
}

func (s DeleteBuilder) ToSql(Dialect) (sql string, args []interface{}, err error) {
	sql, args, err = s.sq.ToSql()
	if err != nil {
		return
	}

	sql, err = sq.Dollar.ReplacePlaceholders(sql)
	return
}

// *Patch* to squirrel selector to allow consistent setting
// of columns and values.
type UpdateBuilder struct {
	sq sq.UpdateBuilder
}

// This is a patch to fix the squirrel update builder,
// where it only allows a single column to be at a time
func Update(from string) UpdateBuilder {
	return UpdateBuilder{sq.Update(from)}
}

func (u UpdateBuilder) Set(col string, val interface{}) UpdateBuilder {
	return UpdateBuilder{u.sq.Set(col, val)}
}

func (u UpdateBuilder) Where(pred string, binds ...interface{}) UpdateBuilder {
	return UpdateBuilder{u.sq.Where(pred, binds...)}
}

func (u UpdateBuilder) SetAll(columns Columns, vals ...interface{}) UpdateBuilder {
	num := len(vals)
	if num != len(columns) {
		panic(fmt.Sprintf("Expected [%v] colums, got [%v]", num, len(columns)))
	}

	sq := u.sq
	for i := 0; i < num; i++ {
		sq = sq.Set(columns[i], vals[i])
	}
	return UpdateBuilder{sq}
}

func (s UpdateBuilder) ToSql(Dialect) (sql string, binds []interface{}, err error) {
	sql, args, err := s.sq.ToSql()
	if err != nil {
		return
	}

	binds = make([]interface{}, 0, len(args))
	for _, val := range args {
		switch t := val.(type) {
		case driver.Valuer, time.Time, *time.Time:
			binds = append(binds, val)
			continue
		case encoding.BinaryMarshaler:
			bytes, err := t.MarshalBinary()
			if err != nil {
				return "", nil, err
			}

			binds = append(binds, bytes)
			continue
		case encoding.TextMarshaler:
			bytes, err := t.MarshalText()
			if err != nil {
				return "", nil, err
			}
			binds = append(binds, string(bytes))
			continue
		case json.Marshaler:
			bytes, err := t.MarshalJSON()
			if err != nil {
				return "", nil, err
			}

			binds = append(binds, string(bytes))
			continue
		}

		if reflect.ValueOf(val).Kind() == reflect.Struct {
			var bytes []byte
			if err := enc.Json.EncodeBinary(val, &bytes); err != nil {
				return "", nil, err
			}

			binds = append(binds, bytes)
			continue
		}

		binds = append(binds, val)
	}

	sql, err = sq.Dollar.ReplacePlaceholders(sql)
	return
}

// Returns a raw sql query
func Raw(sql string, bindings ...interface{}) Query {
	return QueryFn(func(d Dialect) (query string, binds []interface{}, err error) {
		return sql, bindings, nil
	})
}

// Returns a raw sql query from a format string.  This differs from Raw
// in that it will not generate/allow bindings
func Rawf(format string, args ...interface{}) Query {
	return QueryFn(func(d Dialect) (query string, binds []interface{}, err error) {
		return fmt.Sprintf(format, args...), []interface{}{}, nil
	})
}

func SelectIn(vals ...interface{}) string {
	var strs []string
	for _, v := range vals {
		strs = append(strs, pq.QuoteIdentifier(fmt.Sprintf("%v", v)))
	}
	return fmt.Sprintf("%v", strings.Join(strs, ","))
}

func SelectStrings(vals ...string) string {
	var args []interface{}
	for _, v := range vals {
		args = append(args, v)
	}
	return SelectIn(args...)
}

func SelectUUIDs(vals ...uuid.UUID) string {
	var args []interface{}
	for _, v := range vals {
		args = append(args, v)
	}
	return SelectIn(args...)
}

func InStrings(vals ...string) (args []interface{}) {
	for _, v := range vals {
		args = append(args, v)
	}
	return
}

func InUUIDs(vals ...uuid.UUID) (args []interface{}) {
	for _, v := range vals {
		args = append(args, v)
	}
	return
}

func LowerAll(vals ...string) (args []string) {
	for _, v := range vals {
		args = append(args, strings.ToLower(v))
	}
	return
}
