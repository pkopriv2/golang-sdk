package secret

import (
	"io"

	"github.com/pkopriv2/golang-sdk/lang/crypto"
	"github.com/pkopriv2/golang-sdk/lang/enc"
)

type KeyChain struct {
	Key      crypto.KeyPair          `json:"key"`
	Seed     crypto.SaltedCipherText `json:"seed"`
	Hash     crypto.Hash             `json:"hash"`
	Strength crypto.Strength         `json:"strength"`
}

func NewKeyChain(rand io.Reader, enc enc.Encoder, secret Secret, strength crypto.Strength) (
	chain KeyChain, priv crypto.PrivateKey, seed crypto.Bytes, err error) {

	// generate the chain's private key
	priv, err = strength.GenKey(rand, crypto.RSA)
	if err != nil {
		return
	}

	// generate the chain's encryption seed
	seed, err = strength.GenNonce(rand)
	if err != nil {
		return
	}

	// generate the chain's password (dependent only on the secret)
	pass, err := secret.Hash(strength.Hash())
	if err != nil {
		return
	}

	// generate the chain's encrypted key pair
	encPair, err := strength.GenKeyPair(rand, enc, priv, pass)
	if err != nil {
		return
	}

	encSeed, err := strength.SaltAndEncrypt(rand, pass, seed)
	if err != nil {
		return
	}

	chain = KeyChain{encPair, encSeed, strength.Hash(), strength}
	return
}

func (k KeyChain) ExtractKey(dec enc.Decoder, secret Secret) (ret crypto.PrivateKey, err error) {
	pass, err := secret.Hash(k.Hash)
	if err != nil {
		return
	}
	return k.Key.Decrypt(dec, pass)
}

func (k KeyChain) ExtractSeed(dec enc.Decoder, secret Secret) (ret crypto.Bytes, err error) {
	pass, err := secret.Hash(k.Hash)
	if err != nil {
		return
	}
	return k.Seed.Decrypt(pass)
}

func (k KeyChain) Rotate(rand io.Reader, enc enc.EncoderDecoder, secret Secret) (ret KeyChain, err error) {
	// validates the correct secret (FIXME: Start signing)
	if _, err = k.ExtractKey(enc, secret); err != nil {
		return
	}

	// generate the shared encryption key
	pass, err := secret.Hash(k.Hash)
	if err != nil {
		return
	}
	defer crypto.Destroy(pass)

	// generate the shared key
	nextPlainKey, err := k.Strength.GenKey(rand, crypto.RSA)
	if err != nil {
		return
	}
	defer crypto.Destroy(nextPlainKey)

	// generate the shared encryption key
	nextPlainSeed, err := k.Strength.GenNonce(rand)
	if err != nil {
		return
	}
	defer crypto.Destroy(nextPlainSeed)

	nextKey, err := k.Strength.GenKeyPair(rand, enc, nextPlainKey, pass)
	if err != nil {
		return
	}

	nextSeed, err := k.Strength.SaltAndEncrypt(rand, pass, nextPlainSeed)
	if err != nil {
		return
	}

	ret = KeyChain{nextKey, nextSeed, k.Hash, k.Strength}
	return
}
