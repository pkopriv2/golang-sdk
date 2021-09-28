package enc

import (
	"reflect"

	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkg/errors"
)

type Impls map[string]interface{}

func ReadIface(dec Decoder, in []byte, m Impls, dest interface{}) (err error) {
	typ, err := readType(dec, in)
	if err != nil {
		return
	}

	impl, ok := m[typ]
	if !ok {
		err = errors.Wrapf(errs.ArgError, "Unsupported interface type [%v]", typ)
		return
	}
	if err = readImpl(dec, in, &impl); err != nil {
		return
	}

	target := reflect.ValueOf(dest)
	source := reflect.ValueOf(impl)
	if target.Kind() != reflect.Ptr || source.Kind() != reflect.Ptr {
		err = errors.Wrapf(errs.ArgError, "Must supply a pointer", typ)
		return
	}

	if !source.Type().AssignableTo(target.Type().Elem()) {
		err = errors.Wrapf(errs.ArgError, "Incompatible target type [%v]. Expected [%v]", target.Type(), source.Type())
		return
	}

	target.Elem().Set(source)
	return
}

func WriteIface(enc Encoder, typ string, val interface{}) (ret []byte, err error) {
	err = enc.EncodeBinary(struct {
		Type string      `json:"type"`
		Impl interface{} `json:"data"`
	}{
		typ, val,
	}, &ret)
	return
}

func readType(dec Decoder, in []byte) (ret string, err error) {
	err = dec.DecodeBinary(in, &struct {
		Type *string `json:"type"`
	}{
		&ret,
	})
	return
}

func readImpl(dec Decoder, in []byte, ptr interface{}) error {
	return dec.DecodeBinary(in, &struct {
		Impl interface{} `json:"data"`
	}{
		ptr,
	})
}
