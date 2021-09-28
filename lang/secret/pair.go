package secret

import (
	"io"

	"github.com/pkopriv2/golang-sdk/lang/crypto"
	"github.com/pkopriv2/golang-sdk/lang/enc"
)

const (
	SecretPairFormat = "secret-pair/0.0.1"
)

type PublicShard struct {
	Shard
	Format   string
	Hash     crypto.Hash
	Strength crypto.Strength
}

type publicShard struct {
	Format   *string          `json:"format,omitempty"`
	Shard    Shard            `json:"shard"`
	Hash     *crypto.Hash     `json:"hash"`
	Strength *crypto.Strength `json:"strength"`
}

func (s PublicShard) MarshalJSON() (ret []byte, err error) {
	err = enc.Json.EncodeBinary(publicShard{&s.Format, &EncodableShard{s.Shard}, &s.Hash, &s.Strength}, &ret)
	return
}

func (s *PublicShard) UnmarshalJSON(in []byte) (err error) {
	tmp := EncodableShard{}
	err = enc.Json.DecodeBinary(in, &publicShard{&s.Format, &tmp, &s.Hash, &s.Strength})
	s.Shard = tmp.Shard
	return
}

func (s PublicShard) DeriveSecret(dec enc.Decoder, shard PrivateShard, pass []byte) (ret Secret, err error) {
	priv, err := shard.Decrypt(dec, pass)
	if err != nil {
		return
	}

	ret, err = s.Shard.Derive(priv)
	return
}

func (s PublicShard) DerivedSecret(dec enc.Decoder, priv PrivateShard, secret Secret) (ret Secret, err error) {
	pass, err := secret.Hash(priv.EncryptedShard.Salt.Hash) // need to pull hash from priv
	if err != nil {
		return
	}
	ret, err = s.DeriveSecret(dec, priv, pass)
	return
}

// Generates a new public and private shard pair.
func NewSecretPair(rand io.Reader, enc enc.Encoder, secret Secret, pass []byte, strength crypto.Strength) (pub PublicShard, priv PrivateShard, err error) {
	plainPub, err := secret.Shard(rand)
	if err != nil {
		return
	}

	plainPriv, err := secret.Shard(rand)
	if err != nil {
		return
	}
	defer crypto.Destroy(plainPriv)
	priv, err = EncryptShard(rand, enc, plainPriv, pass, strength)
	if err != nil {
		return
	}

	pub = PublicShard{plainPub, SecretPairFormat, strength.Hash(), strength}
	return
}

// A PrivateShard is a durable EncryptedShard.
type PrivateShard struct {
	EncryptedShard `json:shard`
	Format         string `json:format,omitempty`
}

// Signs the shard, returning a signed shard.
func EncryptShard(rand io.Reader, enc enc.Encoder, shard Shard, pass []byte, strength crypto.Strength) (ret PrivateShard, err error) {
	salt, err := crypto.GenSalt(rand, strength.SaltOptions())
	if err != nil {
		return
	}

	raw, err := EncodeShard(enc, shard)
	if err != nil {
		return
	}

	ct, err := salt.Encrypt(rand, strength.Cipher(), pass, raw)
	if err != nil {
		return
	}

	ret = PrivateShard{EncryptedShard(ct), SecretPairFormat}
	return
}

func EncryptShardBySecret(rand io.Reader, enc enc.Encoder, shard Shard, sec Secret, strength crypto.Strength) (ret PrivateShard, err error) {
	pass, err := sec.Hash(strength.Hash())
	if err != nil {
		return
	}

	ret, err = EncryptShard(rand, enc, shard, pass, strength)
	return
}

// Convenience methods for dealing with encrypted shards.
func DecryptShardBySecret(dec enc.Decoder, shard EncryptedShard, sec Secret) (ret Shard, err error) {
	pass, err := sec.Hash(shard.Salt.Hash)
	if err != nil {
		return
	}

	ret, err = shard.Decrypt(dec, pass)
	return
}

// Convenience methods for dealing with encrypted shards.
func DeriveSecretBySecret(dec enc.Decoder, pub PublicShard, shard EncryptedShard, sec Secret) (ret Shard, err error) {
	pass, err := sec.Hash(shard.Salt.Hash)
	if err != nil {
		return
	}

	ret, err = shard.Decrypt(dec, pass)
	return
}
