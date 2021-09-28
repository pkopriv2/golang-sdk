package crypto

import (
	"io"
)

type SaltOptions struct {
	Hash Hash
	Iter int
	Size int
}

type SaltOption func(*SaltOptions)

func buildSaltOptions(fns ...SaltOption) SaltOptions {
	ret := SaltOptions{SHA256, 1024, 32}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

func WithSaltHash(hash Hash) SaltOption {
	return func(s *SaltOptions) {
		s.Hash = hash
	}
}

func WithSaltIterations(iter int) SaltOption {
	return func(s *SaltOptions) {
		s.Iter = iter
	}
}

func WithSaltSize(size int) SaltOption {
	return func(s *SaltOptions) {
		s.Size = size
	}
}

// A Salt is a source of randomness that may be used to improve the
// randomness of hashing functions.  A salt, in combination with
// another random value (ie a key), is intended to be used as the
// basis for secure cipher keys.
//
type Salt struct {
	Hash  Hash   `json:"hash"`
	Iter  int    `json:"iter"`
	Nonce []byte `json:"nonce"`
}

// Generates a new salt.  Additional options may be specified, but
// a one argument constructor is available for those who would like
// reasonable defaults.
func GenSalt(rand io.Reader, fns ...SaltOption) (ret Salt, err error) {
	opts := buildSaltOptions(fns...)

	nonce, err := GenNonce(rand, opts.Size)
	if err != nil {
		return
	}

	ret = Salt{opts.Hash, opts.Iter, nonce}
	return
}

func (s Salt) Apply(val Bytes, size int) Bytes {
	return val.Pbkdf2(s.Nonce, s.Iter, size, s.Hash)
}

func (s Salt) Encrypt(rand io.Reader, cipher Cipher, key, msg Bytes) (ret SaltedCipherText, err error) {
	ciphertext, err := cipher.Apply(rand, s.Apply(key, cipher.KeySize()), msg)
	if err != nil {
		return
	}

	ret = SaltedCipherText{ciphertext, s}
	return
}

// A SaltedCipherText has been encrypted with a key that has had a
// salt applied to it.
type SaltedCipherText struct {
	CipherText `json:"ciphertext"`
	Salt       Salt `json:"salt"`
}

func (s SaltedCipherText) Decrypt(key Bytes) (Bytes, error) {
	return s.CipherText.Decrypt(s.Salt.Apply(key, s.Cipher.KeySize()))
}
