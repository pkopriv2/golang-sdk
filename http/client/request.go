package client

import (
	"fmt"
	"io"
	"reflect"

	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/http/headers"
)

func Get(path string, args ...interface{}) Request {
	return BuildRequest(WithMethod("GET"), WithPath(path, args...))
}

func Head(path string, args ...interface{}) Request {
	return BuildRequest(WithMethod("HEAD"), WithPath(path, args...))
}

func Put(path string, args ...interface{}) Request {
	return BuildRequest(WithMethod("PUT"), WithPath(path, args...))
}

func Post(path string, args ...interface{}) Request {
	return BuildRequest(WithMethod("POST"), WithPath(path, args...))
}

func Delete(path string, args ...interface{}) Request {
	return BuildRequest(WithMethod("DELETE"), WithPath(path, args...))
}

func BuildRequest(fns ...Request) Request {
	return func(r RequestBuilder) (err error) {
		for _, fn := range fns {
			if err = fn(r); err != nil {
				return
			}
		}
		return
	}
}

func (r Request) And(all ...Request) Request {
	return func(b RequestBuilder) (err error) {
		if err = r(b); err != nil {
			return err
		}
		for _, fn := range all {
			if err = fn(b); err != nil {
				return
			}
		}
		return
	}
}

func WithBearer(token string) Request {
	return WithHeader(headers.Authorization, fmt.Sprintf("Bearer %v", token))
}

func WithMethod(method string) Request {
	return func(r RequestBuilder) (err error) {
		r.SetMethod(method)
		return
	}
}

func WithPath(fmt string, args ...interface{}) Request {
	return func(r RequestBuilder) (err error) {
		r.SetPath(fmt, args...)
		return
	}
}

// Adds query parameter to the request. Nil values
// are ignored.
func WithQueryParam(name string, val interface{}) Request {
	return func(r RequestBuilder) (err error) {
		if v := reflect.ValueOf(val); v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return
			}

			val = v.Elem().Interface()
		}

		r.SetQuery(name, fmt.Sprintf("%v", val))
		return
	}
}

func WithHeader(name string, val interface{}) Request {
	return func(r RequestBuilder) (err error) {
		if v := reflect.ValueOf(val); v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return
			}

			val = v.Elem().Interface()
		}

		r.SetHeader(name, fmt.Sprintf("%v", val))
		return
	}
}

func WithBody(mime string, val []byte) Request {
	return func(r RequestBuilder) (err error) {
		r.SetBody(mime, val)
		return
	}
}

func WithBodyRaw(mime string, val io.Reader) Request {
	return func(r RequestBuilder) (err error) {
		r.SetBodyRaw(mime, val)
		return
	}
}

func WithStruct(enc enc.Encoder, val interface{}) Request {
	return func(r RequestBuilder) (err error) {
		var raw []byte
		if err = enc.EncodeBinary(val, &raw); err != nil {
			return
		}
		r.SetBody(enc.Mime(), raw)
		return
	}
}

func Either(val, def interface{}) interface{} {
	if reflect.Zero(reflect.TypeOf(val)).Interface() == val {
		return def
	} else {
		return val
	}
}
