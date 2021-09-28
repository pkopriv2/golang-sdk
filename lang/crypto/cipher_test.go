package crypto

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncrypt(t *testing.T) {
	t.Run("KeyTooSmall", func(t *testing.T) {
		_, err := Encrypt(rand.Reader, AES_128_GCM, []byte{}, []byte("msg"))
		assert.NotNil(t, err)
	})

	t.Run("KeyTooLarge", func(t *testing.T) {
		_, err := Encrypt(rand.Reader, AES_128_GCM, make([]byte, 24), []byte("msg"))
		assert.NotNil(t, err)
	})

	key, err := GenNonce(rand.Reader, bits128)
	assert.Nil(t, err)

	ct, err := Encrypt(rand.Reader, AES_128_GCM, key, []byte("hello, world"))
	assert.Nil(t, err)

	raw, err := ct.Decrypt(key)
	assert.Nil(t, err)
	assert.Equal(t, Bytes([]byte("hello, world")), raw)

	//salt, err := GenSalt(rand.Reader)
	//assert.Nil(t, err)

	//sct, err := salt.Encrypt(rand.Reader, AES_128_GCM, []byte("key"), []byte("hello, world"))
	//assert.Nil(t, err)

	//msg, err := sct.Decrypt([]byte("key"))
	//assert.Nil(t, err)
	//assert.Equal(t, msg, Bytes([]byte("hello, world")))

	//_, err = sct.Decrypt([]byte("bad"))
	//assert.NotNil(t, err)
}
