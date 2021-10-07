package server

import (
	"fmt"
	"io"
	"net/http"

	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/http/headers"
	"github.com/pkopriv2/golang-sdk/lang/mime"
)

var (
	StatusOK                 = Reply(WithCode(http.StatusOK), WithMessage("Ok"))
	StatusCreated            = Reply(WithCode(http.StatusCreated), WithMessage("Created"))
	StatusNoContent          = Reply(WithCode(http.StatusNoContent))
	StatusAccepted           = Reply(WithCode(http.StatusAccepted), WithMessage("Accepted"))
	StatusFound              = Reply(WithCode(http.StatusFound), WithMessage("Found"))
	StatusNotFound           = Reply(WithCode(http.StatusNotFound), WithMessage("Not Found"))
	StatusNoMethod           = Reply(WithCode(http.StatusMethodNotAllowed), WithMessage("Method Not Allowed"))
	StatusBadRequest         = Reply(WithCode(http.StatusBadRequest), WithMessage("Bad Request"))
	StatusPrecondition       = Reply(WithCode(http.StatusPreconditionFailed), WithMessage("Precondition Failed"))
	StatusConflict           = Reply(WithCode(http.StatusConflict), WithMessage("Conflict"))
	StatusPreconditionFailed = Reply(WithCode(http.StatusPreconditionFailed), WithMessage("PreconditionFailed"))
	StatusTimeout            = Reply(WithCode(http.StatusRequestTimeout), WithMessage("Found"))
	StatusForbidden          = Reply(WithCode(http.StatusForbidden), WithMessage("Forbidden"))
	StatusUnauthorized       = Reply(WithCode(http.StatusUnauthorized), WithMessage("Unauthorized"))
	StatusPanic              = Reply(WithCode(http.StatusInternalServerError), WithMessage("Internal Server Error"))
)

func Empty() Response {
	return StatusNoContent
}

func Ok(enc enc.Encoder, body interface{}) Response {
	return Reply(StatusOK, WithStruct(enc, body))
}

func Panic(err error) Response {
	return Reply(StatusPanic, WithMessage(err.Error()))
}

func NotFound(err error) Response {
	return Reply(StatusNotFound, WithMessage(err.Error()))
}

func NoMethod(err error) Response {
	return Reply(StatusNoMethod, WithMessage(err.Error()))
}

func BadRequest(err error) Response {
	return Reply(StatusBadRequest, WithMessage(err.Error()))
}

func Unauthorized(err error) Response {
	return Reply(StatusUnauthorized, WithMessage(err.Error()))
}

func Conflict(err error) Response {
	return Reply(StatusConflict, WithMessage(err.Error()))
}

func PreconditionFailed(err error) Response {
	return Reply(StatusPreconditionFailed, WithMessage(err.Error()))
}

func WithCode(code int) Response {
	return func(r ResponseBuilder) (err error) {
		r.SetCode(code)
		return
	}
}

func WithHeader(name, val string) Response {
	return func(r ResponseBuilder) (err error) {
		r.SetHeader(name, val)
		return
	}
}

func WithMessage(msg string) Response {
	return func(r ResponseBuilder) (err error) {
		r.SetBody(mime.Text, []byte(msg))
		return
	}
}

func WithContent(mime string, body io.Reader) Response {
	return func(r ResponseBuilder) (err error) {
		r.SetBodyRaw(mime, body)
		return
	}
}

func WithStruct(enc enc.Encoder, val interface{}) Response {
	return func(r ResponseBuilder) (err error) {
		var raw []byte
		if err = enc.EncodeBinary(val, &raw); err != nil {
			return
		}

		r.SetBody(enc.Mime(), raw)
		return
	}
}

func WithBearer(token string) Response {
	return func(r ResponseBuilder) (err error) {
		r.SetHeader(headers.Authorization, fmt.Sprintf("Bearer %v", token))
		return
	}
}

func Reply(fns ...Response) Response {
	return func(r ResponseBuilder) (err error) {
		for _, fn := range fns {
			if err = fn(r); err != nil {
				return
			}
		}
		return
	}
}
