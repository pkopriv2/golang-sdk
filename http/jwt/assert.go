package jwt

import (
	"fmt"
	"strings"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/http/headers"
	"github.com/pkopriv2/golang-sdk/lang/crypto"
	"github.com/pkopriv2/golang-sdk/lang/errs"
)

type Verifier func(claim interface{}) error

func Assert(req headers.Headers, pub crypto.PublicKey, ptr jwt.Claims, v Verifier) (err error) {
	token, err := ReadToken(req, pub, ptr)
	if token == nil || err != nil {
		err = errs.Or(err, errors.Wrapf(errs.ArgError, "Invalid token"))
		return
	}

	return v(token.Claims)
}

// Composition primitives.
func Or(all ...Verifier) Verifier {
	return func(act interface{}) error {
		ret := make([]string, 0, 8)
		for _, cur := range all {
			if err := cur(act); err != nil {
				ret = append(ret, err.Error())
				continue
			}
			return nil
		}
		return errors.New(fmt.Sprintf("None of the following held: %v", strings.Join(ret, ", ")))
	}
}

func And(all ...Verifier) Verifier {
	return func(act interface{}) error {
		for _, cur := range all {
			if err := cur(act); err != nil {
				return err
			}
		}
		return nil
	}
}
