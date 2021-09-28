package crypto

import "github.com/pkopriv2/golang-sdk/lang/enc"

type EncodableKey struct {
	PublicKey
}

func (e *EncodableKey) UnmarshalJSON(in []byte) error {
	return enc.ReadIface(enc.Json, in, enc.Impls{string(RSA): &RSAPublicKey{}}, &e.PublicKey)
}

func (e EncodableKey) MarshalJSON() ([]byte, error) {
	return enc.WriteIface(enc.Json, string(e.PublicKey.Type()), e.PublicKey)
}

func EncodePublicKey(enc enc.Encoder, p PublicKey) (ret []byte, err error) {
	err = enc.EncodeBinary(&EncodableKey{p}, &ret)
	return
}

func DecodePublicKey(enc enc.Decoder, in []byte) (ret PublicKey, err error) {
	tmp := &EncodableKey{}
	err = enc.DecodeBinary(in, &tmp)
	ret = tmp.PublicKey
	return
}

func ParsePublicKey(enc enc.Decoder, in []byte, key *PublicKey) (err error) {
	*key, err = DecodePublicKey(enc, in)
	return
}
