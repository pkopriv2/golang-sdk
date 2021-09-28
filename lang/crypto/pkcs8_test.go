package crypto

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPKCS8(t *testing.T) {
	key, err := GenRSAKey(rand.Reader, 1024)
	assert.Nil(t, err)

	t.Run("Marshal/Unmarshal", func(t *testing.T) {
		ret, err := MarshalPemPrivateKey(key,
			NewEncryptedPKCS8Encoder(rand.Reader, []byte("pass"), Moderate))
		if !assert.Nil(t, err) {
			return
		}

		parsed, err := UnmarshalPemPrivateKey(ret,
			NewEncryptedPKCS8Decoder([]byte("pass")))
		if !assert.Nil(t, err) {
			return
		}

		assert.Equal(t, key, parsed)
	})

	t.Run("Unmarshal_BadPass", func(t *testing.T) {
		ret, err := MarshalPemPrivateKey(key,
			NewEncryptedPKCS8Encoder(rand.Reader, []byte("pass"), Moderate))
		if !assert.Nil(t, err) {
			return
		}

		_, err = UnmarshalPemPrivateKey(ret,
			NewEncryptedPKCS8Decoder([]byte("bad")))
		if !assert.NotNil(t, err) {
			return
		}
	})
}
