package enc

import (
	"io"

	"gopkg.in/yaml.v2"
)

var (
	Yaml = &YamlEncoder{}
)

type YamlEncoder struct{}

func (j *YamlEncoder) Mime() string {
	return "application/yaml"
}

func (j *YamlEncoder) EncodeBinary(v interface{}, body *[]byte) (err error) {
	*body, err = yamlBytes(v)
	return
}

func (j *YamlEncoder) DecodeBinary(raw []byte, v interface{}) error {
	return parseYamlBytes(raw, v)
}

func (j *YamlEncoder) StreamDecoder(r io.Reader) StreamDecoder {
	panic("Not implemented")
}

func (j *YamlEncoder) StreamEncoder(w io.Writer) StreamEncoder {
	panic("Not implemented")
}

func yamlBytes(v interface{}) (ret []byte, err error) {
	ret, err = yaml.Marshal(v)
	return
}

func parseYamlBytes(raw []byte, v interface{}) (err error) {
	err = yaml.Unmarshal(raw, v)
	return
}
