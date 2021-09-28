package crypto

import (
	"fmt"
	"io"
)

// A KeyExchange contains the data necessary to exchange a
// symmetric cipher key via a public/private key pair.
type KeyExchange struct {
	Type   KeyType `json:"type"`
	Cipher Cipher  `json:"cipher"`
	Hash   Hash    `json:"hash"`
	Data   Bytes   `json:"data"`
}

func GenKeyExchange(rand io.Reader, pub PublicKey, cipher Cipher, hash Hash) (data KeyExchange, key []byte, err error) {
	key, err = initRandomSymmetricKey(rand, cipher)
	if err != nil {
		return
	}

	encKey, err := pub.Encrypt(rand, hash, key)
	if err != nil {
		return
	}

	data = KeyExchange{pub.Type(), cipher, hash, encKey}
	return
}

func (k KeyExchange) DecryptKey(rand io.Reader, priv PrivateKey) (ret []byte, err error) {
	return priv.Decrypt(rand, k.Hash, k.Data)
}

func (c KeyExchange) String() string {
	return fmt.Sprintf("KeyExchange(alg=%v,key=%v,val=%v)", c.Type, c.Hash, c.Data)
}
