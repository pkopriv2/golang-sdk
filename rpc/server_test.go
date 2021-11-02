package rpc

import (
	"os"
	"testing"

	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/stretchr/testify/assert"
)

func TestServer_EmptyResponse(t *testing.T) {
	ctx := context.NewContext(os.Stdout, context.Debug)
	defer ctx.Close()

	type r struct {
		Success bool
	}

	server, err := Serve(ctx, BuildHandlers(WithHandler("func", func(req Request) Response {
		return NewStructResponse(enc.Gob, r{true})
	})), WithEncoder(enc.Gob))
	if !assert.Nil(t, err) {
		return
	}
	defer server.Close()

	client, err := server.Connect()
	if !assert.Nil(t, err) {
		return
	}

	resp, err := client.Send(Request{Func: "func"})
	if !assert.Nil(t, err) {
		return
	}

	var act r
	if err := enc.Gob.DecodeBinary(resp.Body, &act); err != nil {
		return
	}
	assert.True(t, resp.Ok)
	//fmt.Println(enc.Json.MustEncodeString(act))
}
