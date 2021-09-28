package crypto

import (
	"encoding/asn1"
	"encoding/pem"
	"io"

	"github.com/pkg/errors"
)

// Implements the PKCS8 Encoding/Encrypting standard for private keys per
// https://tools.ietf.org/html/rfc5208 and https://tools.ietf.org/html/rfc5958

// Associated resources:
//  - PKCS#5: https://tools.ietf.org/html/rfc2898
const (
	encryptedPKCS8Type = "ENCRYPTED PRIVATE KEY"
)

var (
	oidRSA       = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 1, 1}
	oidPBKDF2    = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 5, 12}
	oidPBES2     = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 5, 13}
	oidSHA256    = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 2, 1}
	oidSHA512    = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 2, 2}
	oidAES128GCM = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 1, 6}
	oidAES192GCM = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 1, 27}
	oidAES256GCM = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 1, 46}
)

// Marshaled from encryptedPrivateKeyInfo#EncryptedData
type privateKeyInfo struct {
	Version             int
	PrivateKeyAlgorithm asn1.ObjectIdentifier
	PrivateKey          []byte
}

type encryptedPrivateKeyInfo struct {
	EncryptionAlgorithm pbes2Algorithms
	EncryptedData       []byte
}

type pbes2Algorithms struct {
	Id     asn1.ObjectIdentifier
	Params pbes2Params
}

type pbes2Params struct {
	KeyDeriviationFunc pbkdf2Algorithms
	EncryptionScheme   encryptionAlgorithms
}

type pbkdf2Algorithms struct {
	Id     asn1.ObjectIdentifier
	Params pbkdf2Params
}

type pbkdf2Params struct {
	Salt           []byte
	IterationCount int
	KeyLength      int
	Prf            asn1.ObjectIdentifier
}

type encryptionAlgorithms struct {
	Id asn1.ObjectIdentifier
	IV []byte
}

func NewEncryptedPKCS8Encoder(rand io.Reader, pass []byte, strength Strength) PemKeyEncoder {
	return func(key PrivateKey) (ret *pem.Block, err error) {
		pass = Bytes(pass).Copy()

		raw, err := key.MarshalBinary() // uses PKCS1
		if err != nil {
			return
		}

		pt, err := asn1.Marshal(
			privateKeyInfo{
				Version:             0,
				PrivateKeyAlgorithm: oidRSA,
				PrivateKey:          raw,
			})
		if err != nil {
			return
		}

		ct, err := strength.SaltAndEncrypt(rand, pass, pt)
		if err != nil {
			return
		}

		bytes, err := asn1.Marshal(
			encryptedPrivateKeyInfo{
				EncryptionAlgorithm: pbes2Algorithms{
					Id: oidPBES2,
					Params: pbes2Params{
						EncryptionScheme: encryptionAlgorithms{
							Id: cipherOid(ct.Cipher),
							IV: ct.Nonce,
						},
						KeyDeriviationFunc: pbkdf2Algorithms{
							Id: oidPBKDF2,
							Params: pbkdf2Params{
								Salt:           ct.Salt.Nonce,
								IterationCount: ct.Salt.Iter,
								KeyLength:      ct.Cipher.KeySize(),
								Prf:            hashOid(ct.Salt.Hash),
							},
						},
					},
				},
				EncryptedData: ct.Data,
			})
		if err != nil {
			return
		}

		ret = &pem.Block{
			Type:  encryptedPKCS8Type,
			Bytes: bytes,
		}
		return
	}
}

// Returns a pem decoder capable of decrypting an encrypted private key -
// whose plaintext format is the same as DefaultPemDecoder
func NewEncryptedPKCS8Decoder(pass []byte) PemKeyDecoder {
	return func(blk *pem.Block, ptr *PrivateKey) (err error) {
		pass = Bytes(pass).Copy()

		enc := encryptedPrivateKeyInfo{}
		if _, err = asn1.Unmarshal(blk.Bytes, &enc); err != nil {
			return
		}

		cipher, err := parseCipherOid(enc.
			EncryptionAlgorithm.
			Params.
			EncryptionScheme.
			Id)
		if err != nil {
			return
		}

		hash, err := parseHashOid(enc.
			EncryptionAlgorithm.
			Params.
			KeyDeriviationFunc.
			Params.
			Prf)
		if err != nil {
			return
		}

		ct := SaltedCipherText{
			CipherText{
				Cipher: cipher,
				Nonce: enc.
					EncryptionAlgorithm.
					Params.
					EncryptionScheme.
					IV,
				Data: enc.EncryptedData,
			},
			Salt{
				Hash: hash,
				Iter: enc.
					EncryptionAlgorithm.
					Params.
					KeyDeriviationFunc.
					Params.
					IterationCount,
				Nonce: enc.
					EncryptionAlgorithm.
					Params.
					KeyDeriviationFunc.
					Params.
					Salt,
			},
		}

		pt, err := ct.Decrypt(pass)
		if err != nil {
			err = errors.Wrapf(err, "Unable to extract private key")
			return
		}

		dat := privateKeyInfo{}
		if _, err = asn1.Unmarshal(pt, &dat); err != nil {
			return
		}

		kt, err := parseKeyTypeOid(dat.PrivateKeyAlgorithm)
		if err != nil {
			return
		}

		*ptr, err = newEmptyPrivateKey(KeyType(kt))
		if err != nil {
			return
		}

		err = (*ptr).UnmarshalBinary(dat.PrivateKey)
		return
	}
}

func keyTypeOid(k KeyType) (ret asn1.ObjectIdentifier) {
	switch k {
	default:
		panic(errors.Wrapf(ErrPemEncoding, "Unsupported keytype [%v]", k))
	case RSA:
		ret = oidRSA
	}
	return
}

func parseKeyTypeOid(id asn1.ObjectIdentifier) (ret KeyType, err error) {
	if id.Equal(oidRSA) {
		ret = RSA
		return
	}
	err = errors.Wrapf(ErrPemEncoding, "Unsupported hash algorithm [%v]", id)
	return
}

func hashOid(h Hash) (ret asn1.ObjectIdentifier) {
	switch h {
	default:
		panic(errors.Wrapf(ErrPemEncoding, "Unsupported hash algorithm [%v]", h))
	case SHA256:
		ret = oidSHA256
	case SHA512:
		ret = oidSHA512
	}
	return
}

func parseHashOid(id asn1.ObjectIdentifier) (ret Hash, err error) {
	if id.Equal(oidSHA256) {
		ret = SHA256
		return
	}
	if id.Equal(oidSHA512) {
		ret = SHA512
		return
	}
	err = errors.Wrapf(ErrPemEncoding, "Unsupported hash algorithm [%v]", id)
	return
}

func cipherOid(c Cipher) (ret asn1.ObjectIdentifier) {
	switch c {
	default:
		panic(errors.Wrapf(ErrPemEncoding, "Unsupported cipher [%v]", c))
	case AES_128_GCM:
		ret = oidAES128GCM
	case AES_192_GCM:
		ret = oidAES192GCM
	case AES_256_GCM:
		ret = oidAES256GCM
	}
	return
}

func parseCipherOid(id asn1.ObjectIdentifier) (ret Cipher, err error) {
	if id.Equal(oidAES128GCM) {
		ret = AES_128_GCM
		return
	}
	if id.Equal(oidAES192GCM) {
		ret = AES_192_GCM
		return
	}
	if id.Equal(oidAES256GCM) {
		ret = AES_256_GCM
		return
	}

	err = errors.Wrapf(ErrPemEncoding, "Unsupported cipher [%v]", id)
	return
}
