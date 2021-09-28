package server

import (
	"os"
	"testing"

	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/env"
	"github.com/pkopriv2/golang-sdk/lang/http/client"
	"github.com/stretchr/testify/assert"
)

func TestServer_EmptyResponse(t *testing.T) {
	ctx := context.NewContext(os.Stdout, context.Debug)
	defer ctx.Close()

	server, err := Serve(ctx, func(s *Service) {
		s.Register(Get("/test"), func(e env.Environment, r Request) (ret Response) {
			return StatusOK
		})
		s.Register(Get("/test/v2"), func(e env.Environment, r Request) (ret Response) {
			return StatusPanic
		})
	})
	if err != nil {
		t.FailNow()
	}
	defer server.Close()

	assert.Nil(t,
		server.Connect().Call(
			client.Get("/test"),
			client.ExpectCode(200)))
}

func TestServer_NonEmptyResponse(t *testing.T) {
	ctx := context.NewContext(os.Stdout, context.Debug)
	defer ctx.Close()

	type Return struct {
		A int `json:"a"`
	}

	server, err := Serve(ctx, func(s *Service) {
		s.Register(Get("/test"), func(e env.Environment, r Request) (ret Response) {
			return Ok(enc.Json, Return{1})
		})
	})
	if err != nil {
		t.FailNow()
	}
	defer server.Close()

	var ret Return
	assert.Nil(t,
		server.Connect().Call(
			client.Get("/test"),
			client.ExpectAll(
				client.ExpectCode(200),
				client.ExpectStruct(enc.DefaultRegistry, &ret))))
	assert.Equal(t, Return{1}, ret)

}
