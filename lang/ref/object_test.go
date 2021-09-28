package ref

import (
	"fmt"
	"testing"

	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/stretchr/testify/assert"
)

func TestObjectGet_JSON(t *testing.T) {

	str := `
{
"key1": "val1",
"key2": {
"key21": "val21"
}
}
`

	obj, err := ReadObject(enc.Json, []byte(str))
	if !assert.Nil(t, err) {
		return
	}
	var typed map[string]string

	ok, err := obj.Get("#/key2", MapOf(String), &typed)
	if !assert.Nil(t, err) || !assert.True(t, ok) {
		return
	}

	var val string
	ok, err = obj.Get("#/key2/key21", String, &val)
	if !assert.Nil(t, err) || !assert.True(t, ok) {
		return
	}

	assert.Equal(t, val, "val21")
}

func TestYaml_Single(t *testing.T) {
	var val int
	// Yaml.DecodeBinary([]byte("val"), &val)
	fmt.Println(enc.Json.DecodeBinary([]byte(`0`), &val))
	fmt.Println(val)
}

func TestObjectGet_YAML(t *testing.T) {

	str := `
---
key1: val1
key2:
  key21: val21
key3: null
`

	obj, err := ReadObject(enc.Yaml, []byte(str))
	if !assert.Nil(t, err) {
		return
	}

	var typed map[string]string

	if assert.False(t, obj.Has("#/none")) {
		return
	}
	if assert.True(t, obj.Has("#/")) {
		return
	}
	if assert.True(t, obj.Has("#/key1")) {
		return
	}
	if assert.True(t, obj.Has("#/key2")) {
		return
	}
	if assert.True(t, obj.Has("#/key2/key21")) {
		return
	}

	ok, err := obj.Get("#/none", MapOf(String), &typed)
	if !assert.Nil(t, err) || !assert.False(t, ok) {
		return
	}

	ok, err = obj.Get("#/key2", MapOf(String), &typed)
	if !assert.Nil(t, err) || !assert.True(t, ok) {
		return
	}

	ok, err = obj.Get("#/key3", MapOf(String), &typed)
	if !assert.Nil(t, err) || !assert.False(t, ok) {
		return
	}

	assert.Equal(t, map[string]string{"key21": "val21"}, typed)

	var val string
	ok, err = obj.Get("#/key2/key21", String, &val)
	if !assert.Nil(t, err) || !assert.True(t, ok) {
		return
	}

	assert.Equal(t, val, "val21")

	obj.Set("#/new/new2", 11)
	fmt.Println(enc.Json.MustEncodeString(obj))
}

func TestArray_YAML(t *testing.T) {

	str := `
---
key1:
  - val11
  - val12
`

	obj, err := ReadObject(enc.Yaml, []byte(str))
	if !assert.Nil(t, err) {
		return
	}

	var vals []string

	ok, err := obj.Get("#/key1", ArrayOf(String), &vals)
	if !assert.Nil(t, err) || !assert.True(t, ok) {
		return
	}

	fmt.Println("VALS: ", vals)
}
