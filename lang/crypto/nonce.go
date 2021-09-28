package crypto

import (
	"io"

	"github.com/pkg/errors"
)

// Creates a new random nonce of size bytes.
func GenNonce(rand io.Reader, size int) (Bytes, error) {
	arr := make([]byte, size)
	if _, err := io.ReadFull(rand, arr); err != nil {
		return nil, errors.WithStack(err)
	}
	return arr, nil
}
