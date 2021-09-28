package client

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkopriv2/golang-sdk/lang/http/headers"
)

var tlsMatch = regexp.MustCompile(":443$")

type HttpClient struct {
	Raw     *http.Client
	Proto   string
	Address string
}

func NewDefaultClient(addr string) (ret Client) {
	var proto string
	if tlsMatch.MatchString(addr) {
		proto = "https"
	} else {
		proto = "http"
	}
	ret = &HttpClient{http.DefaultClient, proto, addr}
	return
}

func (h *HttpClient) url(rel string, queries map[string]string) (ret *url.URL, err error) {
	ret, err = url.Parse(fmt.Sprintf("%v://%v/%v", h.Proto, h.Address, strings.Trim(rel, "/")))
	if err != nil {
		return
	}

	query := ret.Query()
	for k, v := range queries {
		query.Set(k, url.QueryEscape(v))
	}

	ret.RawQuery = query.Encode()
	return
}

func (h *HttpClient) Call(reqFn Request, respFn func(Response) error) (err error) {
	data, err := buildRequest(reqFn)
	if err != nil {
		return
	}

	url, err := h.url(data.path, data.queries)
	if err != nil {
		return
	}

	req, err := http.NewRequest(data.method, url.String(), data.body)
	if err != nil {
		return
	}

	for k, v := range data.headers {
		req.Header.Set(k, v)
	}

	raw, err := h.Raw.Do(req)
	if err != nil {
		err = fmt.Errorf("Error calling [%v %v]: %w", data.method, url, err)
		return
	}

	res := &response{raw}
	defer func() {
		err = errs.Or(err, res.Close())
	}()
	if err = respFn(res); err != nil {
		err = fmt.Errorf("Error calling [%v %v]: %w", data.method, url, err)
	}
	return
}

type requestBuilder struct {
	path    string
	method  string
	headers map[string]string
	queries map[string]string
	body    io.Reader
}

func buildRequest(req Request) (ret *requestBuilder, err error) {
	ret = &requestBuilder{headers: make(map[string]string), queries: make(map[string]string)}
	if err = req(ret); err != nil {
		return
	}
	return
}

func (h *requestBuilder) SetMethod(m string) {
	h.method = m
}

func (h *requestBuilder) SetPath(p string, args ...interface{}) {
	escaped := make([]interface{}, 0, len(args))
	for _, arg := range args {
		escaped = append(escaped, url.PathEscape(fmt.Sprintf("%v", arg)))
	}

	h.path = fmt.Sprintf(p, escaped...)
}

func (h *requestBuilder) SetQuery(k, v string) {
	h.queries[k] = v
}

func (h *requestBuilder) SetHeader(k, v string) {
	h.headers[k] = v
}

func (h *requestBuilder) SetBody(mime string, val []byte) {
	h.SetHeader(headers.ContentType, mime)
	h.body = bytes.NewBuffer(val)
}

func (h *requestBuilder) SetBodyRaw(mime string, val io.Reader) {
	h.SetHeader(headers.ContentType, mime)
	h.body = val
}

type response struct {
	raw *http.Response
}

func (h response) Close() error {
	return h.raw.Body.Close()
}

func (h response) ReadCode() int {
	return h.raw.StatusCode
}

func (h response) ReadHeader(key string, val *string) (ok bool) {
	*val = h.raw.Header.Get(key)
	defer func() {
		ok = *val != ""
	}()
	return
}

func (h response) ReadBody(ptr *[]byte) (err error) {
	*ptr, err = ioutil.ReadAll(h.raw.Body)
	return
}

func (h response) Read(p []byte) (n int, err error) {
	return h.raw.Body.Read(p)
}

func (h response) String() string {
	return fmt.Sprintf("%v", h.raw)
}
