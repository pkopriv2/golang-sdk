package sql

import (
	"fmt"

	"github.com/pkg/errors"
)

var (
	ErrMissingMigration = errors.New("Sql:MissingMigration")
)

// A schema is the structural definition of a single database table.
//
// In addition to a declaration of the desired 'current'/'latest'
// state, the schema also contains instructions for migrating from
// past states up to the current state.
//
// The schema uses an epochal/monotonic time model - meaning migrations
// must form a continuos timeline from the starting state to the
// desired state.  If no such timeline exists, this schema is incapable
// of migrating.
//
type Schema struct {
	Name    string
	Version int
	Columns []Column
	Indices []Index
	Deltas  map[int]Atomic
}

func NewSchema(name string, version int) *Schema {
	return &Schema{Name: name, Version: version, Deltas: make(map[int]Atomic)}
}

func (t *Schema) WithColumns(columns ...Column) *Schema {
	t.Columns = columns
	return t
}

func (t *Schema) WithIndices(indices ...Index) *Schema {
	t.Indices = indices
	return t
}

func (t *Schema) WithMigration(from int, fn Atomic) *Schema {
	t.Deltas[from] = fn
	return t
}

func (t *Schema) WithStruct(src interface{}) *Schema {
	return t.WithColumns(structColumns(src)...)
}

func (t *Schema) Build() Schema {
	return *t
}

func (t Schema) Migrations(beg, end int) (ret map[int]Atomic, err error) {
	ret = make(map[int]Atomic)
	if beg >= end {
		return
	}
	for ; beg < end; beg++ {
		fn, ok := t.Deltas[beg]
		if !ok {
			err = errors.Wrapf(ErrMissingMigration, "Could not find migration [%v]", beg)
			return
		}

		ret[beg] = fn
	}
	return
}

func (t Schema) Insert(val interface{}) InsertBuilder {
	return InsertInto(t.Name).Struct(val)
}

func (t Schema) Select() SelectBuilder {
	return SelectSchema(t)
}

func (t Schema) SelectAs(as string) SelectBuilder {
	return SelectSchemaAs(t, as)
}

func (t Schema) Update() UpdateBuilder {
	return Update(t.Name)
}

func (t Schema) Delete() DeleteBuilder {
	return DeleteFrom(t.Name)
}

func (t Schema) Cols() Columns {
	ret := make([]string, 0, len(t.Columns))
	for _, c := range t.Columns {
		ret = append(ret, c.Name)
	}
	return ret
}

func (t Schema) As(as string) string {
	return fmt.Sprintf("%v as %v", t.Name, as)
}

func (t Schema) Init(tx Tx) (err error) {
	fn := Exec(CreateTable(Table{t.Name, t.Columns}))
	for _, idx := range t.Indices {
		fn = fn.Then(Exec(CreateIndex(t.Name, idx)))
	}

	err = fn(tx)
	return
}

// Returns a create table query.  May be used in a any tx
func CreateTable(t Table) Query {
	return QueryFn(func(d Dialect) (stmt string, _ []interface{}, err error) {
		stmt, err = d.CreateTableStmt(t)
		return
	})
}

// Returns an add index query.
func CreateIndex(table string, i Index) Query {
	return QueryFn(func(d Dialect) (stmt string, _ []interface{}, err error) {
		stmt, err = d.CreateIndexStmt(table, i)
		return
	})
}

// Returns an add column query
func AddColumn(table string, c Column) Query {
	return QueryFn(func(d Dialect) (stmt string, _ []interface{}, err error) {
		stmt, err = d.AddColumnStmt(table, c)
		return
	})
}
