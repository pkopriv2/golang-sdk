package crypto

import (
	"encoding/pem"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkopriv2/golang-sdk/lang/path"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

var (
	ErrPemEncoding = errors.New("Crypto:PemEncoding")
)

// Various key encoding formats.
type PrivateKeyFormat string

const (
	PKCS1          PrivateKeyFormat = "pkcs1"
	PKCS8Encrypted                  = "pkcs8-enc"
)

// Standard pem decoder interface
type PemKeyDecoder func(*pem.Block, *PrivateKey) error

// Standard pem decoder interface
type PemKeyEncoder func(PrivateKey) (*pem.Block, error)

// Marshals a private key into a PEM encoded binary block
func MarshalPemPrivateKey(key PrivateKey, enc PemKeyEncoder) (ret []byte, err error) {
	blk, err := enc(key)
	if err != nil {
		return
	}

	ret = pem.EncodeToMemory(blk)
	return
}

// Parses a binary PEM block into a private key
func UnmarshalPemPrivateKey(raw []byte, fn PemKeyDecoder) (ret PrivateKey, err error) {
	blk, _ := pem.Decode(raw)
	err = fn(blk, &ret)
	return
}

// Reads a PEM-encoded private key from a generic reader
func ReadPrivateKey(r io.Reader, fn PemKeyDecoder) (ret PrivateKey, err error) {
	bytes, err := ioutil.ReadAll(r)
	if err != nil {
		return
	}

	ret, err = UnmarshalPemPrivateKey(bytes, fn)
	return
}

// Reads a PEM-encoded private key from the filesystem
func ReadPrivateKeyFile(file string, fn PemKeyDecoder) (ret PrivateKey, err error) {
	path, err := filepath.Abs(file)
	if err != nil {
		return
	}

	raw, err := os.Open(path)
	if err != nil {
		return
	}
	defer raw.Close()
	ret, err = ReadPrivateKey(raw, fn)
	return
}

// Writes a pem encoded private key onto the filesystem.
func WritePrivateKey(key PrivateKey, w io.Writer, fn PemKeyEncoder) (err error) {
	bytes, err := MarshalPemPrivateKey(key, fn)
	if err != nil {
		return
	}

	_, err = w.Write(bytes)
	return
}

// Writes a pem encoded private key onto the filesystem.
func WritePrivateKeyFile(key PrivateKey, file string, fn PemKeyEncoder) (err error) {
	bytes, err := MarshalPemPrivateKey(key, fn)
	if err != nil {
		return
	}

	file, err = path.Expand(file)
	if err != nil {
		return
	}

	fs, dir := afero.NewOsFs(), path.Dir(file)

	exists, err := afero.Exists(fs, dir)
	if err != nil {
		return
	}

	if !exists {
		if err = fs.MkdirAll(dir, 0755); err != nil {
			return
		}
	}

	err = afero.WriteFile(fs, file, bytes, 0600)
	return
}

// Reads the format of a private key from a pem encoded block
func UnmarshalPrivateKeyFormat(raw []byte) (ret PrivateKeyFormat, err error) {
	blk, _ := pem.Decode(raw)
	if blk == nil {
		err = errors.Wrapf(ErrPemEncoding, "Unable to decode pem from bytes")
		return
	}

	switch blk.Type {
	default:
		err = errors.Wrapf(ErrPemEncoding, "Unsupported pem type")
	case encryptedPKCS8Type:
		ret = PKCS8Encrypted
	case rsaPemType:
		ret = PKCS1
	}
	return
}

// Reads a PEM-encoded private key from the filesystem
func ReadPrivateKeyFormat(r io.Reader) (ret PrivateKeyFormat, err error) {
	bytes, err := ioutil.ReadAll(r)
	if err != nil {
		return
	}

	ret, err = UnmarshalPrivateKeyFormat(bytes)
	return
}

// Reads a PEM-encoded private key from the filesystem
func ReadPrivateKeyFormatFromFile(file string) (ret PrivateKeyFormat, err error) {
	path, err := filepath.Abs(file)
	if err != nil {
		return
	}

	raw, err := os.Open(path)
	if err != nil {
		return
	}
	defer raw.Close()
	ret, err = ReadPrivateKeyFormat(raw)
	return
}
