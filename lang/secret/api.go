package secret

import (
	"io"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/crypto"
	"github.com/pkopriv2/golang-sdk/lang/enc"
)

// Useful references:
//
// * Secret sharing survey:
//		* https://www.cs.bgu.ac.il/~beimel/Papers/Survey.pdf
// * Computational secrecy:
//		* http://www.cs.cornell.edu/courses/cs754/2001fa/secretshort.pdf
// * SSRI (Secure store retrieval and )
//		* http://ac.els-cdn.com/S0304397598002631/1-s2.0-S0304397598002631-main.pdf?_tid=d4544ec8-221e-11e7-9744-00000aacb35f&acdnat=1492290326_2f9f40490893fb853da9d080f5b47634
// * Blakley's scheme
//		* https://en.wikipedia.org/wiki/Secret_sharing#Blakley.27s_scheme

var (
	ErrNoAlgorithm = errors.New("Secret:ErrNoAlgorithm")
)

// An algorithm describes the normalized name of the secret sharing protocol.
type Type string

func (a Type) String() string {
	return string(a)
}

// A Secret is a special value that may be decomposed - or sharded -
// and then reconstituted from a subset of those shards.
type Secret interface {
	crypto.Destroyer
	Hash(crypto.Hash) (crypto.Bytes, error)
	Shard(rand io.Reader) (Shard, error)
}

// A shard represents a piece of a secret.  It may be combined with
// other shards in order to rederive the secret.
type Shard interface {
	crypto.Destroyer
	Type() Type
	Derive(Shard) (Secret, error)
}

// Generates a new random secret.
func GenSecret(rand io.Reader, strength crypto.Strength) (ret Secret, err error) {
	opts := BuildSecretOptions(func(o *SecretOptions) {
		o.Entropy = strength.NonceSize()
	})

	switch opts.Algorithm {
	default:
		err = errors.Wrapf(ErrNoAlgorithm, "No such algorithm [%v]", opts.Algorithm)
		return
	case Lines:
		ret, err = generateLineSecret(rand, opts.Entropy)
		return
	}
}

// An encrypted shard is a shard that has been symmetrically
// encrypted with a password strengthening function (e.g. a salt)
type EncryptedShard crypto.SaltedCipherText

func (p EncryptedShard) Decrypt(dec enc.Decoder, pass []byte) (ret Shard, err error) {
	raw, err := crypto.SaltedCipherText(p).Decrypt(pass)
	if err != nil {
		return
	}

	ret, err = DecodeShard(dec, raw)
	return
}

func (p EncryptedShard) Derive(dec enc.Decoder, pub Shard, pass []byte) (ret Secret, err error) {
	shard, err := p.Decrypt(dec, pass)
	if err != nil {
		return
	}
	defer crypto.Destroy(shard)
	ret, err = shard.Derive(pub)
	return
}
