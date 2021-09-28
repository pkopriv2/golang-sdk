package crypto

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"encoding"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/pkg/errors"
)

var (
	ErrInvalidSignature = errors.New("Crypto:InvalidSignature")
)

// Rand is is a secure random number generator
var Rand = rand.Reader

// A Destroyer allows callers to destroy internal state (especially important
// when handling secure data)
type Destroyer interface {
	Destroy()
}

// A signature is a cryptographically secure structure that may be used to both prove
// the authenticity of an accompanying document, as well as the identity of the signer.
type Signature struct {
	Key  string `json:"key"`
	Hash Hash   `json:"hash"`
	Data []byte `json:"data"`
}

// Verifies the signature with the given public key.  Returns nil if the verification succeeded.
func (s Signature) Verify(key PublicKey, msg []byte) error {
	return key.Verify(s.Hash, msg, s.Data)
}

// Returns a simple string rep of the signature.
func (s Signature) String() string {
	return fmt.Sprintf("Signature(hash=%v): %v", s.Hash, Bytes(s.Data))
}

// A a signable object is one that has a consistent format for signing and verifying.
type Signable interface {
	SigningFormat() string
	SigningBytes() ([]byte, error)
}

// A signer contains the knowledge necessary to digitally sign messages.
//
// For high-security environments and applications, signers should be preferred
// over private keys - as the interface demands nothing in terms of local
// state.
type Signer interface {
	Public() PublicKey
	Sign(rand io.Reader, hash Hash, msg []byte) (Signature, error)
	CryptoSigner() crypto.Signer
}

// Public keys are the basis of identity within the trust
// ecosystem.  In plain english, I don't trust your identity, I
// only trust your keys.  Therefore, risk planning starts with
// limiting the exposure of your trusted keys.  The more trusted
// a key, the greater the implications of a compromised one.
//
// The purpose of using Warden is not just to publish keys for
// your own personal use (although it may be used for that), it
// is ultimately expected to serve as a way that people can form
// groups of trust.
//
type PublicKey interface {
	Signable

	Type() KeyType
	ID() string
	Verify(hash Hash, msg []byte, sig []byte) error
	Encrypt(rand io.Reader, hash Hash, plaintext []byte) ([]byte, error)
	CryptoPublicKey() crypto.PublicKey
}

// Private keys represent proof of ownership of a public key and form the basis of
// how authentication, confidentiality and integrity concerns are managed within the
// ecosystem.
type PrivateKey interface {
	Signer
	Destroyer
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler

	Decrypt(rand io.Reader, hash Hash, ciphertext []byte) ([]byte, error)
	CryptoPrivateKey() crypto.PrivateKey
}

// Converts a raw crypto public key into an internal public key.
func ConvertPublicKey(key crypto.PublicKey) (PublicKey, error) {
	switch typed := key.(type) {
	default:
		return nil, errors.Wrapf(ErrKeyType, "Unsupported key type: %v", reflect.TypeOf(key))
	case *rsa.PublicKey:
		return &RSAPublicKey{typed}, nil
	}
}

// Converts a raw crypto private key into an internal private key.
func ConvertPrivateKey(key crypto.PrivateKey) (PrivateKey, error) {
	switch typed := key.(type) {
	default:
		return nil, errors.Wrapf(ErrKeyType, "Unsupported key type: %v", reflect.TypeOf(key))
	case *rsa.PrivateKey:
		return &RSAPrivateKey{typed, &sync.Mutex{}}, nil
	}
}

// Signs the object with the signer and hash
func Sign(rand io.Reader, obj Signable, signer Signer, hash Hash) (Signature, error) {
	bytes, err := obj.SigningBytes()
	if err != nil {
		return Signature{}, errors.WithStack(err)
	}

	sig, err := signer.Sign(rand, hash, bytes)
	if err != nil {
		return Signature{}, errors.WithStack(err)
	}
	return sig, nil
}

// Verifies the object with the signer and hash
func Verify(obj Signable, key PublicKey, sig Signature) (err error) {
	fmt, err := obj.SigningBytes()
	if err != nil {
		err = errors.WithStack(err)
		return
	}

	if err = key.Verify(sig.Hash, fmt, sig.Data); err != nil {
		err = errors.Wrapf(ErrInvalidSignature, "Invalid signature for key [%v]: %v", key.ID(), err)
		return
	}
	return
}

// Destroys the object.  This is just syntactic sugar.
func Destroy(d Destroyer) {
	d.Destroy()
}
