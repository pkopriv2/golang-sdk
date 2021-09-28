package ref

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRef_Root_EmptyString(t *testing.T) {
	assert.True(t, Ref("").Empty())
}

func TestRef_Root_RootString(t *testing.T) {
	assert.True(t, Ref("#").Empty())
}

func TestRef_Root_Root2String(t *testing.T) {
	assert.True(t, Ref("#/").Empty())
}

func TestRef_Root_RootString_EmptyElems(t *testing.T) {
	assert.True(t, Ref("#////").Empty())
}

func TestPointer_Ref_Empty(t *testing.T) {
	assert.Equal(t, Ref("#"), Pointer("").Ref())
}

func TestPointer_Ref_FileOnly(t *testing.T) {
	assert.Equal(t, Ref("#"), Pointer("file.json").Ref())
}

func TestPointer_Ref_FileAndProtoOnly(t *testing.T) {
	assert.Equal(t, Ref("#"), Pointer("file://file.json").Ref())
}

func TestPointer_Ref_RootOnly(t *testing.T) {
	assert.Equal(t, Ref("#"), Pointer("#").Ref())
}

func TestPointer_Ref_RefOnly(t *testing.T) {
	assert.Equal(t, Ref("#/key"), Pointer("#/key").Ref())
}

func TestPointer_Ref_FileAndRef(t *testing.T) {
	assert.Equal(t, Ref("#/key"), Pointer("file.json#/key").Ref())
}

func TestPointer_Protocol_Empty(t *testing.T) {
	assert.Equal(t, "", Pointer("").Protocol())
}

func TestPointer_Protocol_ProtoOnly(t *testing.T) {
	assert.Equal(t, "proto", Pointer("proto://").Protocol())
}

func TestPointer_Protocol_DocumentOnly(t *testing.T) {
	assert.Equal(t, "", Pointer("doc").Protocol())
}

func TestPointer_Protocol_ProtoAndFile(t *testing.T) {
	assert.Equal(t, "proto", Pointer("proto://doc").Protocol())
}

func TestPointer_Document_Empty(t *testing.T) {
	assert.Equal(t, "", Pointer("").Document())
}

func TestPointer_Document_ProtoOnly(t *testing.T) {
	assert.Equal(t, "", Pointer("proto://").Document())
}

func TestPointer_Document_DocOnly(t *testing.T) {
	assert.Equal(t, "doc", Pointer("doc").Document())
}

func TestPointer_Document_ProtoAndDoc(t *testing.T) {
	assert.Equal(t, "doc", Pointer("proto://doc").Document())
}

func TestPointer_Document_ProtoDocAndRoot(t *testing.T) {
	assert.Equal(t, "doc", Pointer("proto://doc#").Document())
}

func TestPointer_Document_ProtoDocAndRef(t *testing.T) {
	assert.Equal(t, "doc.json", Pointer("proto://doc.json#/key").Document())
}
