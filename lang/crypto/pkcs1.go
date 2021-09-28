package crypto

import (
	"encoding/pem"

	"github.com/pkg/errors"
)

const (
	rsaPemType = "RSA PRIVATE KEY"
)

var (
	PKCS1Encoder PemKeyEncoder = EncodePKCS1
	PKCS1Decoder PemKeyDecoder = DecodePKCS1
)

// Encodes a key in PKCS1 form per https://tools.ietf.org/html/rfc8017
func EncodePKCS1(key PrivateKey) (ret *pem.Block, err error) {
	raw, err := key.MarshalBinary()
	if err != nil {
		return
	}

	var typ string
	switch key.(type) {
	default:
		err = errors.Errorf("Unknown key type [%v]", key)
		return
	case *RSAPrivateKey:
		typ = rsaPemType
	}

	ret = &pem.Block{Type: typ, Bytes: raw}
	return
}

// Decodes a key in PKCS1 form per https://tools.ietf.org/html/rfc8017
func DecodePKCS1(blk *pem.Block, ptr *PrivateKey) (err error) {
	switch blk.Type {
	default:
		err = errors.Errorf("Unknown key type [%v]", blk.Type)
		return
	case rsaPemType:
		*ptr = &RSAPrivateKey{}
	}

	err = (*ptr).UnmarshalBinary(blk.Bytes)
	return
}
