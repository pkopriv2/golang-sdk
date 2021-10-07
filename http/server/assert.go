package server

import (
	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkg/errors"
)

func NotZero(val interface{}, msg string) (ret Response) {
	if err := errs.NotZero(val); err != nil {
		ret = BadRequest(errors.Wrapf(err, msg))
	}
	return
}

func NotNil(val interface{}, msg string) (ret Response) {
	if err := errs.NotNil(val); err != nil {
		ret = BadRequest(errors.Wrapf(err, msg))
	}
	return
}

func AssertTrue(val bool, msg string) (ret Response) {
	if !val {
		ret = BadRequest(errors.Wrapf(errs.ArgError, msg))
	}
	return
}

func First(r ...Response) Response {
	for _, cur := range r {
		if cur != nil {
			return cur
		}
	}
	return nil
}
