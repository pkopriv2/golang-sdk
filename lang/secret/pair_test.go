package secret

import (
	"testing"

	"github.com/pkopriv2/golang-sdk/lang/crypto"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/stretchr/testify/assert"
)

func TestSecretPair(t *testing.T) {
	secret, err := GenSecret(crypto.Rand, crypto.Moderate)
	if !assert.Nil(t, err) {
		return
	}

	pass := []byte("password")

	var priv PrivateShard
	var pub PublicShard
	if !t.Run("NewPair", func(t *testing.T) {
		pub, priv, err = NewSecretPair(crypto.Rand, enc.Json, secret, pass, crypto.Moderate)
		assert.Nil(t, err)
	}) {
		return
	}

	t.Run("DeriveSecret_BadPass", func(t *testing.T) {
		_, err = pub.DeriveSecret(enc.Json, priv, []byte("a"))
		assert.NotNil(t, err)
	})

	t.Run("DeriveSecret", func(t *testing.T) {
		derived, err := pub.DeriveSecret(enc.Json, priv, pass)
		if !assert.Nil(t, err) {
			return
		}
		assert.Equal(t, secret, derived)
	})
}
