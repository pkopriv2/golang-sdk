package jwt

import (
	"strings"

	"github.com/pkopriv2/golang-sdk/lang/crypto"
	"github.com/pkopriv2/golang-sdk/lang/http/headers"
	jwt "github.com/dgrijalva/jwt-go"
)

// Simple alias.
type Claims interface {
	jwt.Claims
}

func NewToken(claims jwt.Claims) *jwt.Token {
	return jwt.NewWithClaims(jwt.SigningMethodPS256, claims)
}

func IssueToken(signer crypto.Signer, claims jwt.Claims) (string, error) {
	return NewToken(claims).SignedString(signer.CryptoSigner())
}

func ReadToken(req headers.Headers, pub crypto.PublicKey, claim jwt.Claims) (ret *jwt.Token, err error) {
	_, err = headers.ParseHeader(req, headers.Authorization, NewTokenDecoder(pub, claim), &ret)
	return
}

func ParseToken(token string, pub crypto.PublicKey, claim jwt.Claims) (ret *jwt.Token, err error) {
	err = NewTokenDecoder(pub, claim)(token, &ret)
	return
}

func NewTokenDecoder(pub crypto.PublicKey, claim jwt.Claims) headers.Decoder {
	return func(val string, raw interface{}) (err error) {
		*raw.(**jwt.Token), err = jwt.ParseWithClaims(strings.Replace(val, "Bearer ", "", 1), claim, func(*jwt.Token) (interface{}, error) {
			return pub.CryptoPublicKey(), nil
		})
		return
	}
}
