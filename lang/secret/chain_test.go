package secret

import (
	"testing"

	"github.com/pkopriv2/golang-sdk/lang/crypto"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/stretchr/testify/assert"
)

func TestKeyRing(t *testing.T) {
	secret, err := GenSecret(crypto.Rand, crypto.Moderate)
	if !assert.Nil(t, err) {
		return
	}

	var ring KeyChain
	var priv crypto.PrivateKey
	var seed crypto.Bytes
	if !t.Run("NewKeyRing", func(t *testing.T) {
		ring, priv, seed, err = NewKeyChain(crypto.Rand, enc.Json, secret, crypto.Moderate)
		assert.Nil(t, err)
	}) {
		return
	}

	t.Run("ExtractKey", func(t *testing.T) {
		derived, err := ring.ExtractKey(enc.Json, secret)
		if !assert.Nil(t, err) {
			return
		}
		assert.Equal(t, priv, derived)
	})

	t.Run("ExtractKey_BadSecret", func(t *testing.T) {
		bad, err := GenSecret(crypto.Rand, crypto.Moderate)
		if !assert.Nil(t, err) {
			return
		}

		_, err = ring.ExtractKey(enc.Json, bad)
		assert.NotNil(t, err)
	})

	t.Run("ExtractSeed", func(t *testing.T) {
		derived, err := ring.ExtractSeed(enc.Json, secret)
		if !assert.Nil(t, err) {
			return
		}
		assert.Equal(t, seed, derived)
	})

	t.Run("ExtractSeed_BadSecret", func(t *testing.T) {
		bad, err := GenSecret(crypto.Rand, crypto.Moderate)
		if !assert.Nil(t, err) {
			return
		}

		_, err = ring.ExtractSeed(enc.Json, bad)
		assert.NotNil(t, err)
	})

	t.Run("Rotate", func(t *testing.T) {
		next, err := ring.Rotate(crypto.Rand, enc.Json, secret)
		if !assert.Nil(t, err) {
			return
		}
		_, err = next.ExtractSeed(enc.Json, secret)
		assert.Nil(t, err)
	})
}
