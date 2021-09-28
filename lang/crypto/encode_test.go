package crypto

import (
	"fmt"
	"testing"

	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/stretchr/testify/assert"
)

func TestKeyEncoding(t *testing.T) {
	key, err := GenRSAKey(Rand, 1024)
	if !assert.Nil(t, err) {
		return
	}

	t.Run("PublicKey", func(t *testing.T) {
		data, err := EncodePublicKey(enc.Json, key.Public())
		if !assert.Nil(t, err) {
			return
		}

		pub, err := DecodePublicKey(enc.Json, data)
		if !assert.Nil(t, err) {
			return
		}

		assert.Equal(t, key.Public(), pub)
	})

	pair, err := NewKeyPair(Rand, enc.Json, key, []byte("password"))
	if !assert.Nil(t, err) {
		return
	}

	t.Run("KeyPair", func(t *testing.T) {
		var data []byte
		if err := enc.Json.EncodeBinary(pair, &data); err != nil {
			t.FailNow()
			return
		}

		fmt.Println(string(data))

		var new KeyPair
		if !assert.Nil(t, enc.Json.DecodeBinary(data, &new)) {
			return
		}

		assert.Equal(t, pair, new)
	})
}
