package enc

import (
	"bytes"
	"io"

	"github.com/BurntSushi/toml"
)

var (
	Toml EncoderDecoder = &TomlEncoder{}
)

type TomlEncoder struct{}

func (t *TomlEncoder) Mime() string {
	return "application/toml"
}

func (t *TomlEncoder) EncodeBinary(v interface{}, body *[]byte) (err error) {
	*body, err = tomlBytes(v)
	return
}

func (t *TomlEncoder) DecodeBinary(raw []byte, v interface{}) error {
	return parseTomlBytes(raw, v)
}

func (j *TomlEncoder) StreamEncoder(r io.Writer) StreamEncoder {
	panic("Not implemented")
}

func (j *TomlEncoder) StreamDecoder(r io.Reader) StreamDecoder {
	panic("Not implemented")
}

func tomlBytes(v interface{}) (ret []byte, err error) {
	var buf bytes.Buffer
	err = toml.NewEncoder(&buf).Encode(v)
	ret = buf.Bytes()
	return
}

func parseTomlBytes(raw []byte, v interface{}) (err error) {
	_, err = toml.Decode(string(raw), v)
	return
}
