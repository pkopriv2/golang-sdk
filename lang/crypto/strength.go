package crypto

import (
	"io"
	"strings"
	"time"

	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkg/errors"
)

var (
	ErrBadStrength = errors.New("Crypto:BadStrength")
)

type Strength int

const (
	Weak Strength = iota
	Minimal
	Moderate
	Strong
	Maximum
)

func ParseStrength(str string) (Strength, error) {
	switch strings.ToLower(str) {
	default:
		return 0, errors.Wrapf(ErrBadStrength, "Invalid strength [%v]", str)
	case "weak":
		return Weak, nil
	case "minimal":
		return Minimal, nil
	case "moderate":
		return Moderate, nil
	case "strong":
		return Strong, nil
	case "maximum":
		return Maximum, nil
	}
}

func (r Strength) String() string {
	switch r {
	default:
		return "Weak"
	case Weak:
		return "Weak"
	case Minimal:
		return "Minimal"
	case Moderate:
		return "Moderate"
	case Strong:
		return "Strong"
	case Maximum:
		return "Maximum"
	}
}

func (s Strength) ClockSkew() time.Duration {
	switch s {
	default:
		return 24 * time.Hour
	case Minimal:
		return 24 * time.Hour
	case Moderate:
		return 1 * time.Hour
	case Strong:
		return 5 * time.Minute
	case Maximum:
		return 1 * time.Minute
	}
}

func (s Strength) TokenTimeout() time.Duration {
	switch s {
	default:
		return 10 * time.Hour
	case Minimal:
		return 24 * time.Hour
	case Moderate:
		return 16 * time.Hour
	case Strong:
		return 10 * time.Minute
	case Maximum:
		return 1 * time.Minute
	}
}

func (s Strength) KeySize() int {
	switch s {
	default:
		return 2048
	case Minimal:
		return 1024
	case Moderate, Strong:
		return 2048
	case Maximum:
		return 4096
	}
}

func (s Strength) NonceSize() int {
	switch s {
	default:
		return 16
	case Minimal:
		return 16
	case Moderate:
		return 32
	case Strong:
		return 48
	case Maximum:
		return 64
	}
}

func (s Strength) KeyExpiration() time.Duration {
	switch s {
	default:
		return 60 * 24 * time.Hour
	}
}

func (s Strength) Hash() Hash {
	switch s {
	default:
		return SHA256
	case Minimal, Moderate, Strong:
		return SHA256
	case Maximum:
		return SHA512
	}
}

func (s Strength) Cipher() Cipher {
	switch s {
	default:
		return AES_128_GCM
	case Minimal:
		return AES_128_GCM
	case Moderate, Strong:
		return AES_192_GCM
	case Maximum:
		return AES_256_GCM
	}
}

func (s Strength) Iterations() int {
	switch s {
	default:
		return 1024
	case Minimal:
		return 1024
	case Moderate:
		return 2048
	case Strong:
		return 4096
	case Maximum:
		return 8192
	}
}

func (s Strength) SaltOptions() func(*SaltOptions) {
	return func(o *SaltOptions) {
		o.Hash = s.Hash()
		o.Size = s.NonceSize()
		o.Iter = s.Iterations()
	}
}

func (s Strength) KeyPairOptions() func(*KeyPairOptions) {
	return func(o *KeyPairOptions) {
		o.Cipher = s.Cipher()
		s.SaltOptions()(&o.SaltOptions)
	}
}

func (s Strength) GenPass(rand io.Reader, opts ...PassOption) (string, error) {
	return GenPass(rand, opts...)
}

func (s Strength) GenDicePass(rand io.Reader) (string, error) {
	return GenDicePass(rand, s)
}

func (s Strength) GenKey(rand io.Reader, alg KeyType) (PrivateKey, error) {
	return GenPrivateKey(alg, rand, s.KeySize())
}

func (s Strength) GenNonce(rand io.Reader) (Bytes, error) {
	return GenNonce(rand, s.NonceSize())
}

func (s Strength) GenSalt(rand io.Reader) (Salt, error) {
	return GenSalt(rand, s.SaltOptions())
}

func (s Strength) GenKeyPair(rand io.Reader, enc enc.Encoder, priv PrivateKey, pass []byte) (KeyPair, error) {
	return NewKeyPair(rand, enc, priv, pass, s.KeyPairOptions())
}

func (s Strength) GenKeyExchange(rand io.Reader, key PublicKey) (KeyExchange, []byte, error) {
	return GenKeyExchange(rand, key, s.Cipher(), s.Hash())
}

func (s Strength) Sign(rand io.Reader, enc enc.Encoder, signer Signer, obj Signable) (Signature, error) {
	return Sign(rand, obj, signer, s.Hash())
}

func (s Strength) Encrypt(rand io.Reader, key, msg []byte) (CipherText, error) {
	return s.Cipher().Apply(rand, key, msg)
}

func (s Strength) SaltAndEncrypt(rand io.Reader, key, msg []byte) (SaltedCipherText, error) {
	salt, err := s.GenSalt(rand)
	if err != nil {
		return SaltedCipherText{}, err
	}
	return salt.Encrypt(rand, s.Cipher(), key, msg)
}
