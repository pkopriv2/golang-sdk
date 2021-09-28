package crypto

import (
	"io"

	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkg/errors"
)

const (
	KeyPairFormat = "keypair/0.1"
)

// Basic options for generating a key/pair
type KeyPairOptions struct {
	SaltOptions
	Cipher Cipher
}

func buildKeyPairOptions(fns ...func(*KeyPairOptions)) KeyPairOptions {
	ret := KeyPairOptions{buildSaltOptions(), AES_256_GCM}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// A SignedKeyPair
type SignedKeyPair struct {
	KeyPair
	Sig Signature
}

func (s SignedKeyPair) Verify(pub PublicKey) (err error) {
	return Verify(s.KeyPair, pub, s.Sig)
}

type signedKeyPair struct {
	Pair *KeyPair   `json:"pair"`
	Sig  *Signature `json:"signature"`
}

func (k SignedKeyPair) MarshalJSON() (ret []byte, err error) {
	err = enc.Json.EncodeBinary(signedKeyPair{&k.KeyPair, &k.Sig}, &ret)
	return
}

func (k *SignedKeyPair) UnmarshalJSON(in []byte) (err error) {
	err = enc.Json.DecodeBinary(in, &signedKeyPair{&k.KeyPair, &k.Sig})
	return
}

// A KeyPair is a PublicKey along with an encrypted PrivateKey.  KeyPairs
// are safe to distribute publicly, but should be done infrequently, if ever.
type KeyPair struct {
	Pub  PublicKey
	Priv SaltedCipherText
}

type keyPair struct {
	Pub  PublicKey         `json:"public"`
	Priv *SaltedCipherText `json:"private"`
}

func (k KeyPair) MarshalJSON() (ret []byte, err error) {
	var pub PublicKey
	if k.Pub != nil {
		pub = &EncodableKey{k.Pub}
	}
	err = enc.Json.EncodeBinary(keyPair{pub, &k.Priv}, &ret)
	return
}

func (k *KeyPair) UnmarshalJSON(in []byte) (err error) {
	tmp := EncodableKey{}
	err = enc.Json.DecodeBinary(in, &keyPair{&tmp, &k.Priv})
	k.Pub = tmp.PublicKey
	return
}

// Generates a new encrypted key pair
func NewKeyPair(rand io.Reader, enc enc.Encoder, priv PrivateKey, pass []byte, fns ...func(*KeyPairOptions)) (ret KeyPair, err error) {
	opts := buildKeyPairOptions(fns...)

	salt, err := GenSalt(rand, func(s *SaltOptions) {
		*s = opts.SaltOptions
	})
	if err != nil {
		return
	}

	var privBytes []byte
	if err = enc.EncodeBinary(priv, &privBytes); err != nil {
		return
	}

	ciphertext, err := salt.Encrypt(rand, opts.Cipher, pass, privBytes)
	if err != nil {
		return
	}

	ret = KeyPair{priv.Public(), ciphertext}
	return
}

func (p KeyPair) SigningFormat() string {
	return KeyPairFormat
}

func (p KeyPair) SigningBytes() ([]byte, error) {
	fmt, err := p.Pub.SigningBytes()
	return fmt, errors.WithStack(err)
}

func (p KeyPair) Sign(rand io.Reader, priv Signer, hash Hash) (ret SignedKeyPair, err error) {
	sig, err := Sign(rand, p, priv, hash)
	if err != nil {
		return
	}

	ret = SignedKeyPair{p, sig}
	return
}

func (p KeyPair) Decrypt(dec enc.Decoder, key []byte) (ret PrivateKey, err error) {
	raw, err := p.Priv.Decrypt(key)
	if err != nil {
		return
	}
	defer raw.Destroy()

	ret, err = newEmptyPrivateKey(p.Pub.Type())
	if err != nil {
		return
	}

	err = dec.DecodeBinary(raw, ret)
	return
}

func (p KeyPair) PubOnly() KeyPair {
	return KeyPair{Pub: p.Pub}
}
