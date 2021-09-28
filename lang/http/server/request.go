package server

import (
	"fmt"

	"github.com/pkopriv2/golang-sdk/lang/http/content"
	"github.com/pkopriv2/golang-sdk/lang/http/headers"
)

type Decoder headers.Decoder

// Common decoders
var (
	ParseStruct   = content.ParseStruct
	RequireStruct = content.RequireStruct
	ParseString   = content.ParseString
	ParseError    = content.ParseError
	ReadHeader    = headers.ReadHeader
	ParseHeader   = headers.ParseHeader
	RequireHeader = headers.RequireHeader
	String        = headers.String
	Bool          = headers.Bool
	Int           = headers.Int
	Uint          = headers.Uint
	Uint64        = headers.Uint64
	Uint32        = headers.Uint32
	Int64         = headers.Int64
	Int32         = headers.Int32
	UUID          = headers.UUID
)

type ParamID func() (string, Decoder, interface{})

func Param(name string, dec Decoder, ptr interface{}) ParamID {
	return func() (string, Decoder, interface{}) {
		return name, dec, ptr
	}
}

func RequirePathParams(req Request, all ...ParamID) (err error) {
	for _, fn := range all {
		name, dec, ptr := fn()
		if err = RequirePathParam(req, name, dec, ptr); err != nil {
			return
		}
	}
	return
}

func RequireQueryParams(req Request, all ...ParamID) (err error) {
	for _, fn := range all {
		name, dec, ptr := fn()
		if err = RequireQueryParam(req, name, dec, ptr); err != nil {
			return
		}
	}
	return
}

func ParseQueryParams(req Request, all ...ParamID) (err error) {
	for _, fn := range all {
		name, dec, ptr := fn()
		if _, err = ParseQueryParam(req, name, dec, ptr); err != nil {
			return
		}
	}
	return
}

func ReadPathParam(req Request, name string, def string) (ret string, err error) {
	ok, err := req.ReadPathParam(name, &ret)
	if err != nil || ok {
		return
	}

	ret = def
	return
}

func ParsePathParam(req Request, name string, dec Decoder, ptr interface{}) (ok bool, err error) {
	var str string
	if ok, err = req.ReadPathParam(name, &str); !ok {
		return
	}
	if err = dec(str, ptr); err != nil {
		err = fmt.Errorf("Error parsing path param [%v]: %w", name, err)
	}
	return
}

func RequirePathParam(req Request, name string, dec Decoder, ptr interface{}) (err error) {
	ok, err := ParsePathParam(req, name, dec, ptr)
	if err != nil {
		return
	}
	if !ok {
		err = fmt.Errorf("Missing required path param [%v]", name)
	}
	return
}

func ParseQueryParam(req Request, name string, dec Decoder, ptr interface{}) (ok bool, err error) {
	var str string
	if ok, err = req.ReadQueryParam(name, &str); err != nil || !ok {
		return
	}
	if err = dec(str, ptr); err != nil {
		err = fmt.Errorf("Error parsing query param [%v]: %w", name, err)
	}
	return
}

func RequireQueryParam(req Request, name string, dec Decoder, ptr interface{}) (err error) {
	ok, err := ParseQueryParam(req, name, dec, ptr)
	if err != nil {
		return
	}
	if !ok {
		err = fmt.Errorf("Missing required query param [%v]", name)
	}
	return
}
