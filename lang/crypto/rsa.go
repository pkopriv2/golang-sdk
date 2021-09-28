package crypto

import (
	"crypto"
	"crypto/rsa"
	"crypto/x509"
	"io"
	"sync"

	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkg/errors"
)

func GenRSAKey(rand io.Reader, bits int) (PrivateKey, error) {
	key, err := rsa.GenerateKey(rand, bits)
	if err != nil {
		return nil, errors.Wrapf(err, "Error generating private key [%v]", bits)
	}
	return &RSAPrivateKey{key, &sync.Mutex{}}, nil
}

// Public RSA key implementation.
type RSAPublicKey struct {
	Raw *rsa.PublicKey
}

func (r *RSAPublicKey) CryptoPublicKey() crypto.PublicKey {
	return r.Raw
}

func (r *RSAPublicKey) Type() KeyType {
	return RSA
}

func (r *RSAPublicKey) ID() string {
	fmt, err := r.MarshalBinary()
	if err != nil {
		panic(err) // not supposed to happen!
	}

	hash, err := Bytes(fmt).Hash(SHA1)
	if err != nil {
		panic(err)
	}
	return hash.Base64()
}

func (r *RSAPublicKey) Verify(hash Hash, msg []byte, sig []byte) error {
	hashed, err := hash.Hash(msg)
	if err != nil {
		return errors.Wrapf(err, "Unable to hash message [%v] using alg [%v]", Bytes(msg), hash)
	}
	if err := rsa.VerifyPSS(r.Raw, hash.crypto(), hashed, sig, nil); err != nil {
		return errors.Wrapf(err, "Unable to verify signature [%v] with key [%v]", Bytes(sig), r.ID())
	}
	return nil
}

func (r *RSAPublicKey) Encrypt(rand io.Reader, hash Hash, msg []byte) ([]byte, error) {
	msg, err := rsa.EncryptOAEP(hash.New(), rand, r.Raw, msg, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "Error encrypting message [%v] with key [%v]", Bytes(msg), r.Raw)
	}
	return msg, nil
}

func (r *RSAPublicKey) MarshalText() (ret []byte, err error) {
	raw, err := r.MarshalBinary()
	if err != nil {
		return
	}

	ret = []byte(Bytes(raw).Base64())
	return
}

func (r *RSAPublicKey) UnmarshalText(text []byte) (err error) {
	raw, err := ParseBase64(string(text))
	if err != nil {
		return
	}

	err = r.UnmarshalBinary(raw)
	return
}

func (r *RSAPublicKey) SigningFormat() string {
	return "X509"
}

func (r *RSAPublicKey) SigningBytes() ([]byte, error) {
	return r.MarshalBinary()
}

func (r *RSAPublicKey) MarshalBinary() (ret []byte, err error) {
	return x509.MarshalPKIXPublicKey(r.Raw)
}

// no need for lock on this one.  the object doesn't exist yet.
func (r *RSAPublicKey) UnmarshalBinary(data []byte) error {
	raw, err := x509.ParsePKIXPublicKey(data)
	if err != nil {
		return err
	}

	pub, ok := raw.(*rsa.PublicKey)
	if !ok {
		return errors.Wrap(errs.ArgError, "Expected a PKIX encoded public key")
	}

	r.Raw = pub
	return nil
}

// Private key implementation
type RSAPrivateKey struct {
	Raw  *rsa.PrivateKey
	lock *sync.Mutex
}

func (r *RSAPrivateKey) Public() PublicKey {
	return &RSAPublicKey{&r.Raw.PublicKey}
}

func (r *RSAPrivateKey) CryptoPrivateKey() crypto.PrivateKey {
	return r.Raw
}

func (r *RSAPrivateKey) CryptoSigner() crypto.Signer {
	return r.Raw
}

func (r *RSAPrivateKey) Sign(rand io.Reader, hash Hash, msg []byte) (Signature, error) {
	hashed, err := hash.Hash(msg)
	if err != nil {
		return Signature{}, errors.Wrapf(err, "Unable to hash message [%v] using alg [%v]", Bytes(msg), hash)
	}

	r.lock.Lock()
	defer r.lock.Unlock()
	sig, err := rsa.SignPSS(rand, r.Raw, hash.crypto(), hashed, nil)
	if err != nil {
		return Signature{}, errors.Wrapf(err, "Unable to sign msg [%v]", Bytes(msg))
	}

	return Signature{r.Public().ID(), hash, sig}, nil
}

func (r *RSAPrivateKey) Decrypt(rand io.Reader, hash Hash, ciphertext []byte) ([]byte, error) {
	plaintext, err := rsa.DecryptOAEP(hash.New(), rand, r.Raw, ciphertext, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to decrypt ciphertext [%v]", Bytes(ciphertext))
	}
	return plaintext, nil
}

func (r *RSAPrivateKey) Destroy() {
	// FIXME: implement
}

func (r *RSAPrivateKey) MarshalText() (text []byte, err error) {
	bin, err := r.MarshalBinary()
	if err != nil {
		return
	}
	text = []byte(Bytes(bin).Base64())
	return
}

func (r *RSAPrivateKey) UnmarshalText(text []byte) (err error) {
	raw, err := ParseBase64(string(text))
	if err != nil {
		return
	}
	err = r.UnmarshalBinary(raw)
	return
}

func (r *RSAPrivateKey) MarshalBinary() ([]byte, error) {
	return x509.MarshalPKCS1PrivateKey(r.Raw), nil
}

func (r *RSAPrivateKey) UnmarshalBinary(data []byte) (err error) {
	priv, err := x509.ParsePKCS1PrivateKey(data)
	if err != nil {
		return
	}

	r.Raw = priv
	r.lock = &sync.Mutex{}
	return
}
