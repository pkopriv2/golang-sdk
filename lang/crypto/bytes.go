package crypto

import (
	"crypto/subtle"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	"golang.org/x/crypto/pbkdf2"
)

// Useful cryptographic binary functions.
type Bytes []byte

func NewBytes(buf []byte) Bytes {
	return Bytes(buf)
}

// Parses a base64 encoded string.
func ParseBase64(raw string) (Bytes, error) {
	return base64.StdEncoding.DecodeString(raw)
}

// Zeroes the underlying byte array.  (Useful for deleting secret information)
func (b Bytes) Destroy() {
	for i := 0; i < len(b); i++ {
		b[i] = 0
	}
}

// Returns a flag indicating whether the byte arrays were byte-wise equivalent.
func (b Bytes) Equals(other Bytes) bool {
	return len(b) == len(other) && subtle.ConstantTimeCompare(b, other) == 1
}

// Returns the length of the underlying array
func (b Bytes) Size() int {
	return len(b)
}

// Returns the length of the underlying array
func (b Bytes) Raw() []byte {
	return []byte(b)
}

// Returns the length of the underlying array
func (b Bytes) Copy() Bytes {
	cop := make([]byte, len(b))
	copy(cop, b)
	return cop
}

// Appends the input byte array onto the target.
func (b Bytes) Concat(other Bytes) Bytes {
	return append(b, other...)
}

// Returns a Base64 representation of the array
func (b Bytes) Base64() string {
	return base64.StdEncoding.EncodeToString(b)
}

// Returns a Base32 representation of the array
func (b Bytes) Base32() string {
	return base32.StdEncoding.EncodeToString(b)
}

// Returns a hex representation of the array
func (b Bytes) Hex() string {
	return hex.EncodeToString(b)
}

// Returns a string representation of the array
func (b Bytes) String() string {
	base64 := b.Base64()
	if b.Size() <= 32 {
		return base64
	} else {
		return fmt.Sprintf("%v... (total=%v)", base64[:32], len(base64))
	}
}

// Returns the hash of the byte array.
func (b Bytes) Hash(h Hash) (Bytes, error) {
	return h.Hash(b)
}

// Applies a PBDKF2 Hash to the byte array.
func (b Bytes) Pbkdf2(salt []byte, iter int, size int, h Hash) Bytes {
	return pbkdf2.Key(b, salt, iter, size, h.New)
}
