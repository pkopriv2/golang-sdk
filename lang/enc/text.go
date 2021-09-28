package enc

var (
	Text EncoderDecoder = &TextEncoder{}
)

type TextEncoder struct{}

func (t *TextEncoder) Mime() string {
	return "application/text"
}

func (t *TextEncoder) EncodeBinary(v interface{}, body *[]byte) (err error) {
	*body = v.([]byte)
	return
}

func (t *TextEncoder) DecodeBinary(raw []byte, v interface{}) (err error) {
	*v.(*[]byte) = raw
	return
}
