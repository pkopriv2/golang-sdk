package client

import (
	"github.com/pkopriv2/golang-sdk/http/content"
	"github.com/pkg/errors"
)

var (
	ErrBadRequest   = errors.New("Http:BadRequest")
	ErrUnauthorized = errors.New("Http:Unauthorized")
	ErrNotFound     = errors.New("Http:NotFound")
	ErrNoMethod     = errors.New("Http:MethodNotAllowed")
	ErrConflict     = errors.New("Http:Conflict")
	ErrPrecondition = errors.New("Http:PreconditionFailed")
)

func ReadError(res Response) (err error) {
	if res == nil {
		return
	}

	var base error
	switch res.ReadCode() {
	default:
		base = content.ErrHttp
	case 400:
		base = ErrBadRequest
	case 401:
		base = ErrUnauthorized
	case 404:
		base = ErrNotFound
	case 405:
		base = ErrNoMethod
	case 409:
		base = ErrConflict
	case 412:
		base = ErrPrecondition
	}

	var msg string
	ParseString(res, &msg)
	err = errors.Wrap(base, msg)
	return
}
