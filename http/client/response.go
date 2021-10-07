package client

import (
	"fmt"
	"net/http"

	"github.com/pkopriv2/golang-sdk/lang/enc"
)

func ExpectAll(fns ...func(Response) error) func(Response) error {
	return func(r Response) (err error) {
		for _, fn := range fns {
			if err = fn(r); err != nil {
				return
			}
		}
		return
	}
}

func ExpectAny(fns ...func(Response) error) func(Response) error {
	return func(r Response) (err error) {
		for _, fn := range fns {
			if err = fn(r); err == nil {
				return
			}
		}
		return
	}
}

func ExpectCode(code int) func(Response) error {
	return func(r Response) (err error) {
		if r.ReadCode() != code {
			if r.ReadCode() >= 400 {
				err = ReadError(r)
			} else {
				err = fmt.Errorf("Expected code [%v].  Got [%v]", code, r.ReadCode())
			}
		}
		return
	}
}

func ExpectMessage(msg string) func(Response) error {
	return func(r Response) (err error) {
		var body []byte
		if err = r.ReadBody(&body); err != nil || body == nil {
			err = fmt.Errorf("Unable to read body [%w], err")
			return
		}

		if msg != string(body) {
			err = fmt.Errorf("Expected message [%v].  Got [%v]", msg, string(body))
		}
		return
	}
}

func ExpectHeader(name string, dec Decoder, ptr interface{}) func(Response) error {
	return func(r Response) (err error) {
		err = RequireHeader(r, name, dec, ptr)
		return
	}
}

func MaybeExpectStruct(reg enc.Registry, found *bool, val interface{}) func(Response) error {
	return func(r Response) (err error) {
		if r.ReadCode() == http.StatusNotFound {
			return
		}

		if r.ReadCode() != http.StatusOK {
			err = ReadError(r)
			return
		}

		*found, err = true, RequireStruct(r, reg, val)
		return
	}
}

func ExpectStruct(reg enc.Registry, val interface{}) func(Response) error {
	return func(r Response) (err error) {
		if r.ReadCode() != http.StatusOK {
			err = fmt.Errorf("Expected code [%v]. Got [%v]", http.StatusOK, r.ReadCode())
			return
		}

		err = RequireStruct(r, reg, val)
		return
	}
}
