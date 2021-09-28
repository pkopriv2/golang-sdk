package secret

import (
	"testing"

	"github.com/pkopriv2/golang-sdk/lang/crypto"
	"github.com/stretchr/testify/assert"
)

func TestSecret(t *testing.T) {
	// priv, e := crypto.GenRsaKey(rand.Reader, 1024)
	// if !assert.Nil(t, e) {
	// return
	// }

	secret, e := GenSecret(crypto.Rand, crypto.Strong)
	if !assert.Nil(t, e) {
		return
	}

	hash, e := secret.Hash(crypto.SHA256)
	if !assert.Nil(t, e) {
		return
	}

	shard, e := secret.Shard(crypto.Rand)
	if !assert.Nil(t, e) {
		return
	}

	pub, e := secret.Shard(crypto.Rand)
	if !assert.Nil(t, e) {
		return
	}

	t.Run("Rederive", func(t *testing.T) {
		s, e := pub.Derive(shard)
		if !assert.Nil(t, e) {
			return
		}

		h, e := s.Hash(crypto.SHA256)
		if !assert.Nil(t, e) {
			return
		}

		assert.Equal(t, hash, h)
	})

	// t.Run("GenerateAndUnlock", func(t *testing.T) {
	// k, e := encryptAndSignShard(rand.Reader, priv, shard, []byte("pass"))
	// assert.Nil(t, e)
	//
	// sh, e := k.Decrypt([]byte("pass"))
	// assert.Nil(t, e)
	//
	// act, e := pub.Derive(sh)
	// assert.Nil(t, e)
	// assert.Equal(t, secret, act)
	// })
}
