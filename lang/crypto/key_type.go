package crypto

import (
	"io"

	"github.com/pkg/errors"
)

var (
	ErrKeyType = errors.New("Crypto:ErrKeyType")
)

// A KeyType represents a supported public/private key pair
// algorithm.
//
// **ECDSA Coming Soon**
type KeyType string

const (
	RSA KeyType = "RSA"
)

func GenPrivateKey(typ KeyType, rand io.Reader, bits int) (ret PrivateKey, err error) {
	switch typ {
	default:
		err = errors.WithStack(ErrKeyType)
		return
	case RSA:
		ret, err = GenRSAKey(rand, bits)
		return
	}
}

func newEmptyPrivateKey(typ KeyType) (ret PrivateKey, err error) {
	switch typ {
	default:
		err = errors.WithStack(ErrKeyType)
		return
	case RSA:
		ret = &RSAPrivateKey{}
		return
	}
}

func newEmptyPublicKey(typ KeyType) (ret PublicKey, err error) {
	switch typ {
	default:
		err = errors.WithStack(ErrKeyType)
		return
	case RSA:
		ret = &RSAPublicKey{}
		return
	}
}
