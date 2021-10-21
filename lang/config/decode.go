package config

import (
	"reflect"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/crypto"
	uuid "github.com/satori/go.uuid"
)

type Decoder func(string, interface{}) error

func UUID(val string, raw interface{}) (err error) {
	id, err := uuid.FromString(val)
	if err != nil {
		return
	}

	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *uuid.UUID:
		*t = id
	case **uuid.UUID:
		*t = &id
	}
	return
}

func String(val string, raw interface{}) (err error) {
	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *string:
		*t = val
	case **string:
		*t = &val
	}
	return
}

func Int(val string, raw interface{}) (err error) {
	i, err := strconv.Atoi(val)
	if err != nil {
		return
	}

	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *int:
		*t = i
	case **int:
		*t = &i
	}
	return
}

func Uint(val string, raw interface{}) (err error) {
	tmp, err := strconv.ParseUint(val, 10, 32)
	if err != nil {
		return
	}

	i := uint(tmp)
	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *uint:
		*t = i
	case **uint:
		*t = &i
	}
	return
}

func Bool(val string, raw interface{}) (err error) {
	tmp, err := strconv.ParseBool(val)
	if err != nil {
		return
	}

	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *bool:
		*t = tmp
	case **bool:
		*t = &tmp
	}
	return
}

func Uint64(val string, raw interface{}) (err error) {
	i, err := strconv.ParseUint(val, 10, 32)
	if err != nil {
		return
	}

	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *uint64:
		*t = i
	case **uint64:
		*t = &i
	}
	return
}

func Uint32(val string, raw interface{}) (err error) {
	tmp, err := strconv.ParseUint(val, 10, 32)
	if err != nil {
		return
	}

	i := uint32(tmp)
	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *uint32:
		*t = i
	case **uint32:
		*t = &i
	}
	return
}

func Int64(val string, raw interface{}) (err error) {
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return
	}

	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *int64:
		*t = i
	case **int64:
		*t = &i
	}
	return
}

func Int32(val string, raw interface{}) (err error) {
	tmp, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		return
	}

	i := int32(tmp)
	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *int32:
		*t = i
	case **int32:
		*t = &i
	}
	return
}

func Duration(val string, raw interface{}) (err error) {
	dur, err := time.ParseDuration(val)
	if err != nil {
		return
	}

	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *time.Duration:
		*t = dur
	case **time.Duration:
		*t = &dur
	}
	return
}

func Strength(val string, raw interface{}) (err error) {
	s, err := crypto.ParseStrength(val)
	if err != nil {
		return
	}

	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *crypto.Strength:
		*t = s
	case **crypto.Strength:
		*t = &s
	}
	return
}
