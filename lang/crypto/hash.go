package crypto

import (
	"crypto"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"hash"

	"github.com/pkg/errors"
)

var (
	ErrUnknownHash = errors.New("Crypto:UnknownHash")
)

// Currently supported hash algorithms.
//
// **Sha3 support coming soon**
const (
	SHA1   Hash = "SHA1"
	SHA256 Hash = "SHA256"
	SHA512 Hash = "SHA512"
)

// Parses the hash algorithm from its standard string format.
func ParseHash(raw string) (ret Hash, err error) {
	ret = Hash(raw)
	switch ret {
	default:
		err = errors.Wrapf(ErrUnknownHash, "Unsupported hash [%v]", raw)
		return
	case SHA1, SHA256, SHA512:
		return
	}
}

// Hash is a convenience type to group all the common hashing functions together.
type Hash string

func (h Hash) New() hash.Hash {
	switch h {
	default:
		return nil
	case SHA1:
		return sha1.New()
	case SHA256:
		return sha256.New()
	case SHA512:
		return sha512.New()
	}
}

func (h Hash) crypto() crypto.Hash {
	switch h {
	default:
		return 0
	case SHA1:
		return crypto.SHA1
	case SHA256:
		return crypto.SHA256
	case SHA512:
		return crypto.SHA512
	}
}

func (h Hash) String() string {
	switch h {
	default:
		return "Unknown"
	case SHA1:
		return "SHA1"
	case SHA256:
		return "SHA256"
	case SHA512:
		return "SHA512"
	}
}

func (h Hash) Hash(msg []byte) ([]byte, error) {
	impl := h.New()
	if _, err := impl.Write(msg); err != nil {
		return nil, errors.WithStack(err)
	}
	return impl.Sum(nil), nil
}
