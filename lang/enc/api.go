package enc

import (
	"strings"
)

var (
	DefaultRegistry = NewRegistry(Json, Toml, Gob, Yaml)
)

func Encode(e Encoder, val interface{}) (ret []byte, err error) {
	err = e.EncodeBinary(val, &ret)
	return
}

type Encoder interface {
	Mime() string
	EncodeBinary(interface{}, *[]byte) error
}

type Decoder interface {
	DecodeBinary([]byte, interface{}) error
}

type EncoderDecoder interface {
	Encoder
	Decoder
}

type StreamEncoder interface {
	Encode(interface{}) error
}

type StreamDecoder interface {
	Decode(interface{}) error
}

type StreamEncoderDecoder interface {
	StreamEncoder
	StreamDecoder
}

type Registry struct {
	mimes map[string]EncoderDecoder
}

func NewRegistry(encoders ...EncoderDecoder) Registry {
	mimes := make(map[string]EncoderDecoder)
	for _, cur := range encoders {
		mimes[cur.Mime()] = cur
	}

	return Registry{mimes}
}

func (d Registry) FindByMime(mime string) (ok bool, ret EncoderDecoder) {
	for _, enc := range d.mimes {
		if strings.HasPrefix(mime, enc.Mime()) {
			return true, enc
		}
	}
	return
}
