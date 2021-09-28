package sql

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

var (
	ErrBadTag      = errors.New("Sql:BadTag")
	ErrInvalidType = errors.New("Sql:ErrInvalidType")
)

func structColumns(src interface{}) []Column {
	fields := structFields(src)

	cols := make([]Column, 0, len(fields))
	for _, f := range fields {
		col, err := createColumn(f)
		if err != nil {
			panic(err)
		}
		cols = append(cols, col)
	}

	return cols
}

func createColumn(f reflect.StructField) (ret Column, err error) {
	tags := extractTags(f)
	var name string
	if len(tags) > 0 {
		name = tags[0]
	} else {
		name, err = detectName(f)
		if err != nil {
			return
		}
	}

	var typ DataType
	if len(tags) > 1 {
		typ, err = parseTypeTag(tags[1])
		if err != nil {
			return
		}
	} else {
		typ, err = detectType(f)
		if err != nil {
			return
		}
	}

	consts := []Constraint{}
	if len(tags) > 2 {
		for _, tag := range tags[2:] {
			c, err := parseConstraintTag(tag)
			if err != nil {
				return ret, err
			}
			consts = append(consts, c)
		}
	}

	ret = NewColumn(name, typ, consts...)
	return
}

func parseTypeTag(t string) (ret DataType, err error) {
	ret = DataType(t)
	switch ret {
	case Bool, String, Bytes, Integer, Float, Time, UUID:
		return
	}
	err = errors.Wrapf(ErrBadTag, "Bad type [%v]", t)
	return
}

func parseConstraintTag(t string) (ret Constraint, err error) {
	switch t {
	default:
		err = errors.Wrapf(ErrBadTag, "Bad constraint [%v]", t)
	case "not null":
		ret = NotNull
	case "unique":
		ret = Unique
	}
	return
}

func toSnake(in string) string {
	runes := []rune(in)
	length := len(runes)

	var out []rune
	for i := 0; i < length; i++ {
		if i > 0 && unicode.IsUpper(runes[i]) {
			out = append(out, '_')
		}
		out = append(out, unicode.ToLower(runes[i]))

	}

	return string(out)
}

func detectName(t reflect.StructField) (ret string, err error) {
	ret = toSnake(t.Name)
	return
}

func detectType(t reflect.StructField) (ret DataType, err error) {
	ret = typeOf(t.Type)
	return
}

func extractTags(f reflect.StructField) []string {
	tag, ok := f.Tag.Lookup("sql")
	if !ok {
		return []string{}
	}
	return strings.Split(tag, ",")
}

func typeOf(t reflect.Type) (ret DataType) {
	switch t.Kind() {
	case reflect.Bool:
		return Bool
	case reflect.String:
		return String
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return Integer
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return Integer
	case reflect.Float32, reflect.Float64:
		return Float
	}

	var bytes []byte
	if reflect.TypeOf(bytes).AssignableTo(t) {
		return Bytes
	}

	var id uuid.UUID
	if reflect.TypeOf(id).AssignableTo(t) {
		return UUID
	}

	var time time.Time
	if reflect.TypeOf(time).AssignableTo(t) {
		return Time
	}

	var bm encoding.BinaryMarshaler
	if t.Implements(reflect.TypeOf(&bm).Elem()) {
		return Bytes
	}

	var tm encoding.TextMarshaler
	if t.Implements(reflect.TypeOf(&tm).Elem()) {
		return String
	}

	var jm json.Marshaler
	if t.Implements(reflect.TypeOf(&jm).Elem()) {
		return String
	}

	if t.Kind() == reflect.Struct {
		return String
	}

	panic(errors.Wrapf(ErrInvalidType, "Bad type [%v]", t))
}

func structValue(src interface{}) reflect.Value {
	val := reflect.ValueOf(src)
	switch val.Kind() {
	default:
		panic("Input must be a struct or pointer to struct")
	case reflect.Struct:
		return val
	case reflect.Ptr:
		return val.Elem()
	}
}

func structByPtr(src interface{}) reflect.Value {
	val := reflect.ValueOf(src)
	if val.Kind() != reflect.Ptr {
		panic(fmt.Sprintf("Expected Ptr.  Got [%v]", val.Kind()))
	}

	elem := val.Elem()
	if elem.Kind() != reflect.Struct {
		panic(fmt.Sprintf("Expected Struct.  Got [%v]", elem.Kind()))
	}
	return elem
}

func structFields(src interface{}) []reflect.StructField {
	val := structValue(src)
	typ := val.Type()
	num := typ.NumField()

	ret := make([]reflect.StructField, 0, num)
	for i := 0; i < num; i++ {
		ret = append(ret, typ.Field(i))
	}
	return ret
}

func structValues(src interface{}) []interface{} {
	val := structValue(src)
	num := val.NumField()
	ret := make([]interface{}, 0, num)
	for i := 0; i < num; i++ {
		field := val.Field(i)
		if !field.CanInterface() {
			continue
		}

		ret = append(ret, field.Interface())
	}
	return ret
}

func structFieldPtrs(dest interface{}) []interface{} {
	return structReflectedFieldPtrs(structByPtr(dest))
}

func structReflectedFieldPtrs(val reflect.Value) []interface{} {
	num := val.NumField()
	ret := make([]interface{}, 0, num)
	for i := 0; i < num; i++ {
		field := val.Field(i)
		if !field.CanAddr() {
			panic(fmt.Sprintf("Cannot take address of %vth field: %v", i, field))
		}
		ret = append(ret, field.Addr().Interface())
	}
	return ret
}
