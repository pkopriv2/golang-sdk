package ref

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/enc"
)

// An object is a container of values
type Object interface {
	Has(r Ref) bool
	Set(Ref, interface{}) error
	Get(r Ref, t DataType, ptr interface{}) (bool, error)
	GetRaw(r Ref) (interface{}, bool)
	GetDefault(r Ref, t DataType, ptr, def interface{}) error
	Require(r Ref, t DataType, ptr interface{}) error
	Must(r Ref, t DataType, ptr interface{})
}

func ReadObject(dec enc.Decoder, data []byte) (obj Object, err error) {
	var tmp map[string]interface{}
	if err = dec.DecodeBinary(data, &tmp); err != nil {
		return
	}

	obj = Map(normalizeMap(tmp))
	return
}

func ReadStreamObject(dec enc.StreamDecoder) (obj Object, err error) {
	var tmp map[string]interface{}
	if err = dec.Decode(&tmp); err != nil {
		return
	}

	obj = Map(normalizeMap(tmp))
	return
}

// A map implements converts map semantics into an object.
type Map map[string]interface{}

func NewEmptyMap() Map {
	return make(map[string]interface{})
}

func (o Map) Raw() map[string]interface{} {
	return o
}

func (o Map) Each(fn func(string, interface{}) error) (err error) {
	for k, v := range o {
		if err = fn(k, v); err != nil {
			return
		}
	}
	return
}

func (o Map) Set(r Ref, val interface{}) (err error) {
	if r.Empty() {
		err = errors.Wrapf(ErrObject, "Empty ref")
		return
	}

	if r.Tail().Empty() {
		o[r.Head()] = val
		return
	}

	mid, ok := o[r.Head()]
	if !ok {
		o[r.Head()] = make(map[string]interface{})
	}

	typed, ok := mid.(map[string]interface{})
	if !ok {
		typed = make(map[string]interface{})
		o[r.Head()] = typed
	}

	err = Map(typed).Set(r.Tail(), val)
	return
}

func (o Map) Has(r Ref) (ok bool) {
	if r.Empty() {
		ok = true
		return
	}

	tmp, ok := o[r.Head()]
	if !ok || r.Tail().Empty() {
		return
	}

	switch typed := tmp.(type) {
	default:
		return
	case map[string]interface{}:
		ok = Map(typed).Has(r.Tail())
		return
	}
}

func (o Map) GetRaw(r Ref) (ret interface{}, ok bool) {
	if r.Empty() {
		ret, ok = o.Raw(), true
		return
	}

	ret, ok = o[r.Head()]
	if !ok || r.Tail().Empty() {
		ok = ret != nil
		return
	}

	switch typed := ret.(type) {
	default:
		return
	case map[string]interface{}:
		ret, ok = Map(typed).GetRaw(r.Tail())
		return
	}
}

func (o Map) Get(r Ref, t DataType, ptr interface{}) (ok bool, err error) {
	raw, ok := o.GetRaw(r)
	if !ok {
		return
	}
	if err = t.Assign(raw, ptr); err != nil {
		err = errors.Wrapf(err, "Error assigning value for ref [%v]", r)
	}
	return
}

func (m Map) GetDefault(r Ref, t DataType, ptr, def interface{}) (err error) {
	ok, err := m.Get(r, t, ptr)
	if err != nil || ok {
		return
	}
	if def != nil {
		err = t.Assign(def, ptr)
	}
	return
}

func (m Map) Require(r Ref, t DataType, ptr interface{}) (err error) {
	ok, err := m.Get(r, t, ptr)
	if err != nil {
		return
	}
	if !ok {
		err = errors.Wrapf(ErrObject, "Missing required value [%v]", r)
		return
	}
	return
}

func (m Map) Must(r Ref, t DataType, ptr interface{}) {
	if err := m.Require(r, t, ptr); err != nil {
		panic(err)
	}
}

func normalizeMap(m interface{}) (ret map[string]interface{}) {
	ret = make(map[string]interface{})
	switch base := m.(type) {
	default:
		panic("Not a map")
	case map[string]interface{}:
		for k, v := range base {
			switch val := v.(type) {
			default:
				ret[k] = val
			case map[string]interface{}:
				ret[k] = normalizeMap(val)
			case map[interface{}]interface{}:
				ret[k] = normalizeMap(val)
			case []interface{}:
				ret[k] = normalizeArray(val)
			}
		}
	case map[interface{}]interface{}:
		for k, v := range base {
			switch val := v.(type) {
			default:
				ret[fmt.Sprint(k)] = val
			case map[string]interface{}:
				ret[fmt.Sprint(k)] = normalizeMap(val)
			case map[interface{}]interface{}:
				ret[fmt.Sprint(k)] = normalizeMap(val)
			case []interface{}:
				ret[fmt.Sprint(k)] = normalizeArray(val)
			}
		}
	}
	return
}

func normalizeArray(arr []interface{}) (ret []interface{}) {
	ret = make([]interface{}, len(arr))
	for i, v := range arr {
		switch base := v.(type) {
		default:
			ret[i] = v
		case []interface{}:
			ret[i] = normalizeArray(base)
		case map[string]interface{}:
			ret[i] = normalizeMap(base)
		case map[interface{}]interface{}:
			ret[i] = normalizeMap(base)
		}
	}
	return
}
