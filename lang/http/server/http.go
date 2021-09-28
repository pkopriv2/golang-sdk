package server

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkopriv2/golang-sdk/lang/http/headers"
	"github.com/gorilla/mux"
)

type request struct {
	raw  *http.Request
	vars map[string]string
}

func newRequest(raw *http.Request) *request {
	return &request{raw, mux.Vars(raw)}
}

func (r *request) Close() error {
	return r.raw.Body.Close()
}

func (r *request) URL() *url.URL {
	return r.raw.URL
}

func (r *request) Method() string {
	return r.raw.Method
}

func (r *request) Remote() string {
	return r.raw.RemoteAddr
}

func (r *request) ReadHeader(name string, ptr *string) (ok bool) {
	*ptr = r.raw.Header.Get(name)
	defer func() {
		ok = *ptr != ""
	}()
	return
}

func (r *request) ReadPathParam(name string, ptr *string) (ok bool, err error) {
	tmp, ok := r.vars[name]
	if !ok {
		return
	}

	*ptr, err = url.PathUnescape(tmp)
	return
}

func (r request) ReadQueryParam(name string, ptr *string) (ok bool, err error) {
	tmp := r.raw.URL.Query().Get(name)
	if tmp == "" {
		return
	}

	*ptr, err = url.QueryUnescape(tmp)
	if err != nil {
		return
	}

	ok = true
	return
}

func (r request) ReadBody(ptr *[]byte) (err error) {
	defer func() {
		err = errs.Or(err, r.Close())
	}()
	*ptr, err = ioutil.ReadAll(r.raw.Body)
	return
}

func (r request) Read(p []byte) (n int, err error) {
	n, err = r.raw.Body.Read(p)
	return
}

type responseBuilder struct {
	code    int
	headers map[string]string
	body    io.Reader
}

func newResponseBuilder() *responseBuilder {
	return &responseBuilder{500, make(map[string]string), nil}
}

func (h *responseBuilder) SetCode(code int) {
	h.code = code
}

func (h *responseBuilder) SetHeader(name string, val string) {
	h.headers[name] = val
}

func (h *responseBuilder) SetBody(mime string, val []byte) {
	h.SetHeader(headers.ContentType, mime+"; charset=utf-8")
	h.body = bytes.NewBuffer(val)
	return
}

func (h *responseBuilder) SetBodyRaw(mime string, r io.Reader) {
	h.SetHeader(headers.ContentType, mime+"; charset=utf-8")
	h.body = r
	return
}

func (h *responseBuilder) Build(resp http.ResponseWriter) (err error) {
	for k, v := range h.headers {
		resp.Header().Set(k, v)
	}

	resp.WriteHeader(h.code)
	if h.body != nil {
		_, err = io.Copy(resp, h.body)
	}
	return
}
