package enc

import (
	"bytes"
	"encoding/json"
	"io"
)

var (
	Json = &JsonEncoder{}
)

type JsonEncoder struct{}

func (j *JsonEncoder) Mime() string {
	return "application/json"
}

func (j *JsonEncoder) StreamEncoder(r io.Writer) StreamEncoder {
	enc := json.NewEncoder(r)
	enc.SetIndent("", "    ")
	enc.SetEscapeHTML(false)
	return enc
}

func (j *JsonEncoder) StreamDecoder(r io.Reader) StreamDecoder {
	dec := json.NewDecoder(r)
	dec.UseNumber()
	return dec
}

func (j *JsonEncoder) EncodeIndent(v interface{}, body *[]byte) (err error) {
	*body, err = json.MarshalIndent(v, "", "    ")
	return
}

func (j *JsonEncoder) EncodeBinary(v interface{}, body *[]byte) (err error) {
	*body, err = jsonBytes(v)
	return
}

func (j *JsonEncoder) EncodeString(v interface{}) (ret string, err error) {
	var bytes []byte
	if err = j.EncodeBinary(v, &bytes); err != nil {
		return
	}

	ret = string(bytes)
	return
}

func (j *JsonEncoder) MustEncodeString(v interface{}) (ret string) {
	ret, err := j.EncodeString(v)
	if err != nil {
		panic(err)
	}
	return
}

func (j *JsonEncoder) DecodeBinary(raw []byte, v interface{}) error {
	return parseJsonBytes(raw, v)
}

func jsonBytes(v interface{}) (ret []byte, err error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "    ")
	enc.SetEscapeHTML(false)
	err = enc.Encode(v)
	ret = buf.Bytes()
	return
}

func parseJsonBytes(raw []byte, v interface{}) error {
	dec := json.NewDecoder(bytes.NewBuffer(raw))
	dec.UseNumber()
	return dec.Decode(v)
}
