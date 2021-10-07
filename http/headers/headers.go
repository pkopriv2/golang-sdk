package headers

import (
	"reflect"
	"strconv"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	uuid "github.com/satori/go.uuid"
)

// Common headers
const (
	Accept            = "Accept"
	AcceptCharset     = "Accept-Charset"
	AcceptEncoding    = "Accept-Encoding"
	AcceptLanguage    = "Accept-Language"
	Authorization     = "Authorization"
	CacheControl      = "Cache-Control"
	ContentLength     = "Content-Length"
	ContentType       = "Content-Type"
	ContentEncoding   = "Content-Encoding"
	ContentLanguage   = "Content-Language"
	ContentLocation   = "Content-Location"
	ContentRange      = "Content-Range"
	Date              = "Date"
	EncodingChunked   = "chunked"
	EncodingCompress  = "compress"
	EncodingDeflate   = "deflate"
	EncodingGzip      = "gzip"
	Expect            = "Expect"
	Expires           = "Expires"
	IfMatch           = "If-Match"
	IfModifiedSince   = "If-Modified-Since"
	IfNoneMatch       = "If-None-Match"
	IfUnmodifiedSince = "If-Unmodified-Since"
	LastModified      = "Last-Modified"
	Location          = "Location"
	UserAgent         = "User-Agent"
	Warning           = "Warning"
)

type Headers interface {
	ReadHeader(string, *string) bool
}

type Decoder func(string, interface{}) error

func ReadHeader(req Headers, name string, def string) (ret string) {
	if ok := req.ReadHeader(name, &ret); ok {
		return
	}

	ret = def
	return
}

func ParseHeader(req Headers, name string, dec Decoder, ptr interface{}) (ok bool, err error) {
	var str string
	if ok = req.ReadHeader(name, &str); !ok {
		return
	}
	if err = dec(str, ptr); err == nil {
		return
	}
	err = errors.Wrapf(errs.ArgError, "Error parsing header [%v]", err)
	return
}

func RequireHeader(req Headers, name string, dec Decoder, ptr interface{}) (err error) {
	ok, err := ParseHeader(req, name, dec, ptr)
	if err != nil {
		return
	}

	if !ok {
		err = errors.Wrapf(errs.ArgError, "Missing required header [%v]", name)
	}
	return
}

func UUID(val string, raw interface{}) (err error) {
	id, err := uuid.FromString(val)
	if err != nil {
		return
	}

	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *uuid.UUID:
		*t = id
	case **uuid.UUID:
		*t = &id
	}
	return
}

func String(val string, raw interface{}) (err error) {
	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *string:
		*t = val
	case **string:
		*t = &val
	}
	return
}

func Int(val string, raw interface{}) (err error) {
	i, err := strconv.Atoi(val)
	if err != nil {
		return
	}

	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *int:
		*t = i
	case **int:
		*t = &i
	}
	return
}

func Uint(val string, raw interface{}) (err error) {
	tmp, err := strconv.ParseUint(val, 10, 32)
	if err != nil {
		return
	}

	i := uint(tmp)
	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *uint:
		*t = i
	case **uint:
		*t = &i
	}
	return
}

func Bool(val string, raw interface{}) (err error) {
	tmp, err := strconv.ParseBool(val)
	if err != nil {
		return
	}

	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *bool:
		*t = tmp
	case **bool:
		*t = &tmp
	}
	return
}

func Uint64(val string, raw interface{}) (err error) {
	i, err := strconv.ParseUint(val, 10, 32)
	if err != nil {
		return
	}

	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *uint64:
		*t = i
	case **uint64:
		*t = &i
	}
	return
}

func Uint32(val string, raw interface{}) (err error) {
	tmp, err := strconv.ParseUint(val, 10, 32)
	if err != nil {
		return
	}

	i := uint32(tmp)
	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *uint32:
		*t = i
	case **uint32:
		*t = &i
	}
	return
}

func Int64(val string, raw interface{}) (err error) {
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return
	}

	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *int64:
		*t = i
	case **int64:
		*t = &i
	}
	return
}

func Int32(val string, raw interface{}) (err error) {
	tmp, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		return
	}

	i := int32(tmp)
	switch t := raw.(type) {
	default:
		err = errors.Errorf("Cannot assign value [%v] to [%v]", val, reflect.ValueOf(raw))
	case *int32:
		*t = i
	case **int32:
		*t = &i
	}
	return
}
