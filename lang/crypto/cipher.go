package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

var (
	ErrUnknownCipher = errors.New("Crypto:UnknownCipher")
	ErrCipherKey     = errors.New("Crypto:CipherKey")
)

// Supported symmetric ciphers.  This library is intended to ONLY offer support ciphers
// that implement the Authenticated Encryption with Associated Data (AEAD)
// standard.  Currently, that only includes the GCM family of streaming modes.
const (
	AES_128_GCM Cipher = "AES_128_GCM"
	AES_192_GCM Cipher = "AES_192_GCM"
	AES_256_GCM Cipher = "AES_256_GCM"
)

// Parses a cipher from its standard string format.
func ParseCipher(raw string) (ret Cipher, err error) {
	ret = Cipher(raw)
	switch ret {
	default:
		err = errors.Wrapf(ErrUnknownCipher, "Unsupported cipher [%v]", raw)
		return
	case AES_128_GCM, AES_192_GCM, AES_256_GCM:
		return
	}
}

// Symmetric Cipher Type.  (FIXME: Switches are getting annoying...)
type Cipher string

func (s Cipher) KeySize() int {
	switch s {
	default:
		panic("UnknownCipher")
	case AES_128_GCM:
		return bits128
	case AES_192_GCM:
		return bits192
	case AES_256_GCM:
		return bits256
	}
}

func (s Cipher) String() string {
	switch s {
	default:
		return ErrUnknownCipher.Error()
	case AES_128_GCM:
		return "AES_128_GCM"
	case AES_192_GCM:
		return "AES_192_GCM"
	case AES_256_GCM:
		return "AES_256_GCM"
	}
}

func (s Cipher) Apply(rand io.Reader, key, msg []byte) (CipherText, error) {
	return Encrypt(rand, s, key, msg)
}

func (s Cipher) NewBlock(key []byte) (cipher.Block, error) {
	return initBlockCipher(s, key)
}

// A CipherText is a symmetrically encrypted message.  The same key
// used to encrypt the message is required to decrypt it.
//
// It may contain an optional plaintext format which may be used
// for decoding purposes.
type CipherText struct {
	Cipher Cipher `json:"cipher"`
	Nonce  []byte `json:"nonce"`
	Data   []byte `json:"data"`
}

// Symmetrically encrypts the message with the given key.  The length
// of the key **MUST** be equal to Cipher#KeySize()
func Encrypt(rand io.Reader, alg Cipher, key, msg []byte) (ret CipherText, err error) {
	block, err := initBlockCipher(alg, key)
	if err != nil {
		return
	}

	strm, err := initStreamCipher(alg, block)
	if err != nil {
		return
	}

	nonce, err := GenNonce(rand, strm.NonceSize())
	if err != nil {
		err = errors.Wrapf(err, "Error generating nonce of [%v] bytes", strm.NonceSize())
		return
	}

	ret = CipherText{alg, nonce, strm.Seal(nil, nonce, msg, nil)}
	return
}

func (c CipherText) Decrypt(key []byte) (Bytes, error) {
	block, err := initBlockCipher(c.Cipher, key)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	strm, err := initStreamCipher(c.Cipher, block)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ret, err := strm.Open(nil, c.Nonce, c.Data, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return ret, nil
}

func (c CipherText) String() string {
	return fmt.Sprintf("CipherText(alg=%v,nonce=%v,data=%v)", c.Cipher, c.Nonce, Bytes(c.Data))
}

// Common bit->byte conversions
const (
	bits128 = 128 / 8
	bits192 = 192 / 8
	bits256 = 256 / 8
)

func initRandomSymmetricKey(rand io.Reader, alg Cipher) ([]byte, error) {
	switch alg {
	default:
		return nil, errors.Wrapf(ErrUnknownCipher, "Unknown cipher: %v", alg)
	case AES_128_GCM:
		return GenNonce(rand, bits128)
	case AES_192_GCM:
		return GenNonce(rand, bits192)
	case AES_256_GCM:
		return GenNonce(rand, bits256)
	}
}

func initBlockCipher(alg Cipher, key []byte) (cipher.Block, error) {
	if err := ensureValidKey(alg, key); err != nil {
		return nil, errors.WithStack(err)
	}

	switch alg {
	default:
		return nil, errors.Wrapf(ErrUnknownCipher, "Unknown cipher: %v", alg)
	case AES_128_GCM, AES_192_GCM, AES_256_GCM:
		return aes.NewCipher(key)
	}
}

func initStreamCipher(alg Cipher, blk cipher.Block) (cipher.AEAD, error) {
	switch alg {
	default:
		return nil, errors.Wrapf(ErrUnknownCipher, "Unknown cipher: %v", alg)
	case AES_128_GCM, AES_192_GCM, AES_256_GCM:
		return cipher.NewGCM(blk)
	}
}

func ensureValidKey(alg Cipher, key []byte) error {
	switch alg {
	default:
		return errors.Wrapf(ErrUnknownCipher, "Unknown cipher: %v", alg)
	case AES_128_GCM:
		return ensureKeySize(bits128, key)
	case AES_192_GCM:
		return ensureKeySize(bits192, key)
	case AES_256_GCM:
		return ensureKeySize(bits256, key)
	}
}

func ensureKeySize(expected int, key []byte) error {
	if expected != len(key) {
		return errors.Wrapf(ErrCipherKey, "Illegal key [%v].  Expected [%v] bytes but got [%v]", key, expected, len(key))
	} else {
		return nil
	}
}
