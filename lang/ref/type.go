package ref

import (
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
)

var (
	Any      = noType{}
	Bool     = goType{"Bool", reflect.TypeOf(false)}
	String   = goType{"String", reflect.TypeOf("")}
	Int      = goType{"Int", reflect.TypeOf(int(0))}
	Float    = goType{"Float", reflect.TypeOf(float32(0))}
	Duration = durType{}
)

func MapOf(vt DataType) DataType {
	return mapType{String, vt}
}

func ArrayOf(vt DataType) DataType {
	return arrType{vt}
}

// A data type is the core structure for assigning configuration
// values to internal go types.  Eventually, this should include
// support for complex data types (ie custom structs)
type DataType interface {
	Name() string
	ValidSrc(interface{}) bool
	ValidDst(interface{}) bool
	Assign(src, dst interface{}) error
}

// Implements an untyped value.  Any type checking is deferred to
// runtime and performs a best-effort approach with respect to
// assignments.  The destination value must be a pointer whose
// value must be assignable or convertable from the source.
type noType struct{}

func (d noType) Name() string {
	return "None"
}

func (d noType) ValidSrc(src interface{}) (ok bool) {
	return true
}

func (d noType) ValidDst(dst interface{}) (ok bool) {
	return true
}

func (d noType) Assign(src, dst interface{}) (err error) {
	srcVal, srcType := reflect.ValueOf(src), reflect.TypeOf(src)
	dstVal, dstType := reflect.ValueOf(dst), reflect.TypeOf(dst)

	if dstType.Kind() != reflect.Ptr {
		err = errors.Errorf("Cannot assign [%v] to [%v]", srcType, dstType)
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

	err = errors.Errorf("Cannot assign [%v] to [%v]", srcType, dstType)
	return
}

// The map type is a strongly typed map structure.
type mapType struct {
	KeyType DataType
	ValType DataType
}

func (m mapType) Name() string {
	return fmt.Sprintf("Map[%v]%v", m.KeyType.Name(), m.ValType.Name())
}

func (m mapType) ValidSrc(src interface{}) (ok bool) {
	val := reflect.ValueOf(src)
	if val.Kind() != reflect.Map {
		return
	}

	for _, key := range val.MapKeys() {
		if !m.KeyType.ValidSrc(key.Interface()) {
			return
		}
		if !m.ValType.ValidSrc(val.MapIndex(key).Interface()) {
			return
		}
	}

	ok = true
	return
}

func (m mapType) ValidDst(dst interface{}) (ok bool) {
	dstType := reflect.TypeOf(dst)
	if dstType.Kind() != reflect.Ptr {
		return
	}

	dstType = dstType.Elem()
	return m.KeyType.ValidDst(reflect.New(dstType.Key()).Interface()) &&
		m.ValType.ValidDst(reflect.New(dstType.Elem()).Interface())
}

func (m mapType) Assign(src interface{}, dst interface{}) (err error) {
	srcVal := reflect.ValueOf(src)
	dstVal := reflect.ValueOf(dst)
	if !m.ValidSrc(src) {
		var msg interface{}
		if srcVal.IsValid() {
			msg = srcVal.Type()
		} else {
			msg = srcVal.String()
		}

		err = errors.Errorf("Invalid src [%v]", msg)
		return
	}
	if !m.ValidDst(dst) {
		var msg interface{}
		if dstVal.IsValid() {
			msg = dstVal.Type()
		} else {
			msg = dstVal.String()
		}

		err = errors.Errorf("Invalid dst [%v]", msg)
		return
	}

	tmp := reflect.MakeMap(dstVal.Type().Elem())
	for _, key := range srcVal.MapKeys() {
		if key.Kind() == reflect.Interface {
			key = key.Elem()
		}
		tmp.SetMapIndex(key, srcVal.MapIndex(key).Elem())
	}

	dstVal.Elem().Set(tmp)
	return
}

// The map type is a strongly typed map structure.
type arrType struct {
	ValType DataType
}

func (m arrType) Name() string {
	return fmt.Sprintf("Array[%v]", m.ValType.Name())
}

func (m arrType) ValidSrc(src interface{}) (ok bool) {
	val := reflect.ValueOf(src)
	if val.Kind() != reflect.Slice {
		return
	}

	for i := 0; i < val.Len(); i++ {
		if !m.ValType.ValidSrc(val.Index(i).Interface()) {
			return
		}
	}

	ok = true
	return
}

func (m arrType) ValidDst(dst interface{}) (ok bool) {
	dstType := reflect.TypeOf(dst)
	if dstType.Kind() != reflect.Ptr {
		return
	}

	dstType = dstType.Elem()
	return m.ValType.ValidDst(reflect.New(dstType.Elem()).Interface())
}

func (m arrType) Assign(src interface{}, dst interface{}) (err error) {
	srcVal := reflect.ValueOf(src)
	dstVal := reflect.ValueOf(dst)
	if !m.ValidSrc(src) {
		var msg interface{}
		if srcVal.IsValid() {
			msg = srcVal.Type()
		} else {
			msg = srcVal.String()
		}

		err = errors.Errorf("Invalid src [%v]", msg)
		return
	}
	if !m.ValidDst(dst) {
		var msg interface{}
		if dstVal.IsValid() {
			msg = dstVal.Type()
		} else {
			msg = dstVal.String()
		}

		err = errors.Errorf("Invalid dst [%v]", msg)
		return
	}

	size := srcVal.Len()

	tmp := reflect.MakeSlice(dstVal.Type().Elem(), size, size)
	for i := 0; i < size; i++ {
		tmp.Index(i).Set(srcVal.Index(i).Elem())
	}

	dstVal.Elem().Set(tmp)
	return
}

type goType struct {
	name string
	raw  reflect.Type
}

func (d goType) Name() string {
	return d.name
}

func (d goType) ValidSrc(src interface{}) (ok bool) {
	srcType := reflect.TypeOf(src)
	if srcType.AssignableTo(d.raw) {
		return true
	}
	if srcType.ConvertibleTo(d.raw) {
		return true
	}
	return
}

func (d goType) ValidDst(dst interface{}) (ok bool) {
	dstType := reflect.TypeOf(dst)
	if dstType.Kind() != reflect.Ptr {
		return
	}
	dstType = dstType.Elem()
	if dstType.AssignableTo(d.raw) {
		return true
	}
	if dstType.ConvertibleTo(d.raw) {
		return true
	}
	return
}

func (d goType) Assign(src, dst interface{}) (err error) {
	srcVal, srcType := reflect.ValueOf(src), reflect.TypeOf(src)
	dstVal, dstType := reflect.ValueOf(dst), reflect.TypeOf(dst)
	if !d.ValidSrc(src) {
		err = errors.Errorf("Invalid src [%v] for [%v]", srcType, dstType)
		return
	}
	if !d.ValidDst(dst) {
		err = errors.Errorf("Invalid dst [%v] for [%v]", dstType, srcType)
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

	err = errors.Errorf("Cannot assign [%v] to [%v]", srcType, dstType)
	return
}

type durType struct{}

func (d durType) Name() string {
	return "Duration"
}

func (d durType) ValidSrc(src interface{}) (ok bool) {
	_, ok = src.(string)
	if !ok {
		_, ok = src.(time.Duration)
	}
	return
}

func (d durType) ValidDst(dst interface{}) (ok bool) {
	_, ok = dst.(*time.Duration)
	return
}

func (d durType) Assign(src interface{}, dst interface{}) (err error) {
	if !d.ValidSrc(src) {
		err = errors.Errorf("Invalid src [%v] for [%v]", src, reflect.TypeOf(dst))
		return
	}
	if !d.ValidDst(dst) {
		err = errors.Errorf("Invalid dst [%v] for [%v]", dst, reflect.TypeOf(src))
		return
	}

	*dst.(*time.Duration), err = time.ParseDuration(src.(string))
	return
}
