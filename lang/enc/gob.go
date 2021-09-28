package enc

import (
	"bytes"
	"encoding/gob"
	"io"
)

var (
	Gob EncoderDecoder = &GobEncoder{}
)

type GobEncoder struct{}

func (g *GobEncoder) Mime() string {
	return "application/gob"
}

func (g *GobEncoder) EncodeBinary(v interface{}, arr *[]byte) (err error) {
	*arr, err = gobBytes(v)
	return
}

func (g *GobEncoder) DecodeBinary(raw []byte, v interface{}) (err error) {
	return parseGobBytes(raw, v)
}

func (j *GobEncoder) StreamEncoder(r io.Writer) StreamEncoder {
	return gob.NewEncoder(r)
}

func (j *GobEncoder) StreamDecoder(r io.Reader) StreamDecoder {
	return gob.NewDecoder(r)
}

func gobBytes(v interface{}) (ret []byte, err error) {
	var buf bytes.Buffer
	err = gob.NewEncoder(&buf).Encode(v)
	ret = buf.Bytes()
	return
}

func parseGobBytes(raw []byte, v interface{}) (err error) {
	return gob.NewDecoder(bytes.NewBuffer(raw)).Decode(v)
}
