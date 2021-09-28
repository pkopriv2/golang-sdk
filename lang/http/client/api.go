package client

import (
	"io"

	"github.com/pkopriv2/golang-sdk/lang/http/content"
	"github.com/pkopriv2/golang-sdk/lang/http/headers"
)

type Decoder = headers.Decoder

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
	Int32         = headers.Int32
	Int64         = headers.Int64
	Uint          = headers.Uint
	Uint32        = headers.Uint32
	Uint64        = headers.Uint64
	UUID          = headers.UUID
)

type Client interface {
	Call(Request, func(Response) error) error
}

type Request func(RequestBuilder) error

type RequestBuilder interface {
	SetMethod(string)
	SetPath(string, ...interface{})
	SetQuery(string, string)
	SetHeader(string, string)
	SetBody(mime string, val []byte)
	SetBodyRaw(mime string, r io.Reader)
}

type Response interface {
	io.Closer
	io.Reader
	ReadCode() int
	ReadHeader(string, *string) bool
	ReadBody(*[]byte) error
}
