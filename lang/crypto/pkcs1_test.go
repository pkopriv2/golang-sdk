package crypto

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPKCS1(t *testing.T) {
	key, err := GenRSAKey(rand.Reader, 1024)
	assert.Nil(t, err)

	t.Run("Marshal/Unmarshal", func(t *testing.T) {
		ret, err := MarshalPemPrivateKey(key, EncodePKCS1)
		if !assert.Nil(t, err) {
			return
		}

		parsed, err := UnmarshalPemPrivateKey(ret, DecodePKCS1)
		if !assert.Nil(t, err) {
			return
		}

		assert.Equal(t, key, parsed)
	})
}
