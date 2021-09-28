package sql

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

var (
	ErrUnassignable = errors.New("Sql:Unassignable")
)

// An object is any value that may be decomposed into a set
// of references.  This is the base unit for selection.
type Object interface {
	Fields() ([]Ref, error)
}

// A new is a constructor for an object.  However, unlike true constructors,
// a new function accepts a raw ptr value.
type New func(interface{}) Object

// A ref is a reference to a value.  Our only interaction
// with it is to assign it a value.  The expected domain
// of inputs is the same as expected for sql.Scanner
type Ref func(interface{}) error

func (a Ref) Scan(v interface{}) error {
	return a(v)
}

func (a Ref) Fields() ([]Ref, error) {
	return []Ref{a}, nil
}

// A nil reference.  This will accept values of any type
func Nil() Ref {
	return func(src interface{}) (err error) {
		return
	}
}

type Refs []Ref

func (a Refs) Fields() ([]Ref, error) {
	return a, nil
}

func (a Refs) Union(refs ...Ref) Refs {
	return append(a, refs...)
}

type Objects []Object

func (o Objects) Fields() (refs []Ref, err error) {
	for _, obj := range o {
		fields, err := obj.Fields()
		if err != nil {
			return nil, err
		}

		refs = append(refs, fields...)
	}
	return
}

func (o Objects) Union(objs ...Object) Object {
	return append(o, objs...)
}

// Returns the union of all the input sets in the form of a column set.
func Union(all ...Object) (ret Object) {
	ret = Objects(all)
	return
}

// A buffer initializes.
type Buffer interface {
	Next() Object
}

// Returns a new sql object.
func Value(ptr interface{}) (ret Object) {
	return fromPtr(reflect.ValueOf(ptr))
}

// Returns a new sql object.
func Struct(ptr interface{}) (ret Object) {
	return fromStruct(reflect.ValueOf(ptr))
}

// Generates a view that will populate the fields of all of the input structs.
// This is intended to be used in join scenarios.
func MultiStruct(val interface{}) (ret Object) {
	fields := structFieldPtrs(val)
	for _, f := range fields {
		if ret == nil {
			ret = Struct(f)
			continue
		}
		ret = Union(ret, Struct(f))
	}
	return
}

func Slice(ptr interface{}, fn New) Buffer {
	return newBuffer(reflect.ValueOf(ptr), fn)
}

type buffer struct {
	proto reflect.Type
	ptr   reflect.Value
	new   New
}

func newBuffer(ptr reflect.Value, new New) (ret buffer) {
	ptrType := ptr.Type()
	if ptrType.Kind() != reflect.Ptr {
		panic(fmt.Sprintf("Expected pointer to slice. Not [%v]", ptrType))
	}
	sliceType := ptrType.Elem()
	if sliceType.Kind() != reflect.Slice {
		panic(fmt.Sprintf("Expected pointer to slice. Not [%v]", ptrType))
	}

	ret = buffer{sliceType.Elem(), ptr, new}
	return
}

func (s buffer) Next() (obj Object) {
	s.ptr.Elem().Set(reflect.Append(s.ptr.Elem(), reflect.New(s.proto).Elem()))
	return s.new(s.ptr.Elem().Index(s.ptr.Elem().Len() - 1).Addr().Interface())
}

// Returns a reference to a value.
func fromPtr(val reflect.Value) (ret Ref) {
	switch t := val.Interface().(type) {
	case *int:
		return setInt(t)
	case *float64:
		return setFloat64(t)
	case *bool:
		return setBool(t)
	case *[]byte:
		return setBytes(t)
	case *string:
		return setString(t)
	case *uuid.UUID:
		return setUUID(t)
	case *time.Time:
		return setTime(t)
	case encoding.BinaryUnmarshaler:
		return setBinaryUnmarshaler(t)
	case encoding.TextUnmarshaler:
		return setTextUnmarshaler(t)
	case json.Unmarshaler:
		return setJsonUnmarshaler(t)
	}

	if val.Kind() == reflect.Ptr {
		if val.Elem().Kind() == reflect.Struct {
			return setStruct(val.Interface())
		}
	}

	return setReflect(val.Interface())
}

func fromStruct(val reflect.Value) (ret Refs) {
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		panic(errors.Wrapf(ErrUnassignable, "Expected struct got [%v]", val.Kind()))
	}

	for i, n := 0, val.NumField(); i < n; i++ {
		ret = append(ret, fromPtr(val.Field(i).Addr()))
	}
	return
}

func fromSlice(val reflect.Value) (ret Refs) {
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Slice {
		panic(errors.Wrapf(ErrUnassignable, "Expected pointer got [%v]", val.Kind()))
	}

	for i, n := 0, val.Len(); i < n; i++ {
		ret = append(ret, fromPtr(val.Index(i)))
	}
	return
}

func setInt(val *int) Ref {
	return func(v interface{}) (err error) {
		switch t := v.(type) {
		default:
			return errors.Wrapf(ErrUnassignable, "Cannot assign [%v] to [%v]", v, reflect.TypeOf(val))
		case nil:
			return
		case int64:
			*val = int(t)
		}
		return
	}
}

func setFloat64(val *float64) Ref {
	return func(v interface{}) (err error) {
		switch t := v.(type) {
		default:
			return errors.Wrapf(ErrUnassignable, "Cannot assign [%v] to [%v]", v, reflect.TypeOf(val))
		case nil:
			return
		case float64:
			*val = t
		case float32:
			*val = float64(t)
		}
		return
	}
}

func setBool(val *bool) Ref {
	return func(v interface{}) (err error) {
		switch t := v.(type) {
		default:
			return errors.Wrapf(ErrUnassignable, "Cannot assign [%v] to [%v]", v, reflect.TypeOf(val))
		case nil:
			return
		case bool:
			*val = t
		case int64:
			*val = t > 0
		}
		return
	}
}

func setBytes(val *[]byte) Ref {
	return func(v interface{}) (err error) {
		switch t := v.(type) {
		default:
			return errors.Wrapf(ErrUnassignable, "Cannot assign [%v] to [%v]", v, reflect.TypeOf(val))
		case nil:
			return
		case []byte:
			*val = append([]byte{}, t...)
			//case string:
			//*val = []byte(t)
		}
		return
	}
}

func setString(val *string) Ref {
	return func(v interface{}) (err error) {
		switch t := v.(type) {
		default:
			return errors.Wrapf(ErrUnassignable, "Cannot assign [%v] to [%v]", v, reflect.TypeOf(val))
		case nil:
			return
		case string:
			*val = t
		case []byte:
			*val = string(t)
		}
		return
	}
}

func setUUID(val *uuid.UUID) Ref {
	return func(v interface{}) (err error) {
		switch t := v.(type) {
		default:
			return errors.Wrapf(ErrUnassignable, "Cannot assign [%v] to [%v]", v, reflect.TypeOf(val))
		case nil:
			return
		case string:
			*val, err = uuid.FromString(t)
		case []byte:
			*val, err = uuid.FromString(string(t))
		}
		return
	}
}

func setTime(val *time.Time) Ref {
	return func(v interface{}) (err error) {
		switch t := v.(type) {
		default:
			return errors.Wrapf(ErrUnassignable, "Cannot assign [%v] to [%v]", v, reflect.TypeOf(val))
		case nil:
			return
		case time.Time:
			*val = t
		case []byte:
			err = val.UnmarshalBinary(t)
		}
		return
	}
}

func setTextUnmarshaler(val encoding.TextUnmarshaler) Ref {
	return func(v interface{}) (err error) {
		switch t := v.(type) {
		default:
			return errors.Wrapf(ErrUnassignable, "Cannot assign [%v] to [%v]", v, reflect.TypeOf(val))
		case nil:
			return
		case []byte:
			return val.UnmarshalText(t)
		case string:
			return val.UnmarshalText([]byte(t))
		}
	}
}

func setBinaryUnmarshaler(val encoding.BinaryUnmarshaler) Ref {
	return func(v interface{}) (err error) {
		switch t := v.(type) {
		default:
			return errors.Wrapf(ErrUnassignable, "Cannot assign [%v] to [%v]", v, reflect.TypeOf(val))
		case nil:
			return
		case []byte:
			return val.UnmarshalBinary(t)
		case string:
			return val.UnmarshalBinary([]byte(t))
		}
	}
}

func setJsonUnmarshaler(val json.Unmarshaler) Ref {
	return func(v interface{}) (err error) {
		switch t := v.(type) {
		default:
			return errors.Wrapf(ErrUnassignable, "Cannot assign [%v] to [%v]", v, reflect.TypeOf(val))
		case nil:
			return
		case []byte:
			return val.UnmarshalJSON(t)
		case string:
			return val.UnmarshalJSON([]byte(t))
		}
	}
}

func setStruct(val interface{}) Ref {
	return func(v interface{}) (err error) {
		switch t := v.(type) {
		default:
			return errors.Wrapf(ErrUnassignable, "Cannot assign [%v] to [%v]", v, reflect.TypeOf(val))
		case nil:
			return
		case []byte:
			err = enc.Json.DecodeBinary(t, val)
		case string:
			err = enc.Json.DecodeBinary([]byte(t), val)
		}
		return
	}
}

func setReflect(dst interface{}) Ref {
	return func(src interface{}) (err error) {
		srcVal, srcType := reflect.ValueOf(src), reflect.TypeOf(src)
		dstVal, dstType := reflect.ValueOf(dst), reflect.TypeOf(dst)
		if dstType.Kind() != reflect.Ptr {
			err = errors.Wrapf(ErrUnassignable, "Expected pointer got [%v]", dstType)
			return
		}

		dstVal, dstType = dstVal.Elem(), dstType.Elem()
		if dstType.AssignableTo(srcType) {
			dstVal.Set(srcVal)
			return
		}
		if srcType.ConvertibleTo(dstType) {
			dstVal.Set(srcVal.Convert(dstType))
			return
		}

		err = errors.Wrapf(ErrUnassignable, "Cannot assign [%v] to [%v]", srcType, dstType)
		return
	}
}

func nils(n int) (ret []interface{}) {
	for i := 0; i < n; i++ {
		ret = append(ret, Nil())
	}
	return
}
