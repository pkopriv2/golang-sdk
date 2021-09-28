package sql

// Type represents the core data type
type DataType string

const (
	Bool    DataType = "bool"
	UUID             = "uuid"
	String           = "string"
	Bytes            = "blob"
	Integer          = "int"
	Float            = "float"
	Time             = "time"
)

func (d DataType) String() string {
	return string(d)
}

// Contraint types enumerate the constraints classes that are supported
// by this library. Implementing dialects must support all these types,
// or document which are NOT supported.
type ConstraintType string

var (
	NotNull = NewConstraint("not null")
	Unique  = NewConstraint("unique")
)

// A constraint is a condition that has been applied to a column.
// Contraints may contain additional arguments which will be supplied
// to the dialect at table creation time. The structure of the args
// is specific to the type in question.
type Constraint struct {
	Type ConstraintType
	Args []interface{}
}

func NewConstraint(typ ConstraintType, args ...interface{}) Constraint {
	return Constraint{Type: typ, Args: args}
}

// A column is a definition of a database table column. Columns must
// have a name and one of the supported data types.  They may also
// optionally specify any additional constraints.
type Column struct {
	Name        string
	Type        DataType
	Constraints []Constraint
}

func NewColumn(name string, typ DataType, cs ...Constraint) Column {
	return Column{name, typ, cs}
}

// An index is a definition of a database index.
type Index struct {
	Name    string
	Columns []string
	Unique  bool
}

func NewIndex(name string, cols ...string) Index {
	return Index{name, cols, false}
}

func NewUniqueIndex(name string, cols ...string) Index {
	return Index{name, cols, true}
}

// A table is the structural definition of a single database table.
type Table struct {
	Name    string
	Columns []Column
}

func NewTable(name string, cols ...Column) Table {
	return Table{Name: name, Columns: cols}
}
