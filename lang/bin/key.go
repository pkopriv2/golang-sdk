package bin

import (
	"bytes"
	"encoding/binary"

	uuid "github.com/satori/go.uuid"
)

type Key []byte

func (k Key) Raw() []byte {
	return []byte(k)
}

func (k Key) Child(child []byte) Key {
	return Key(append([]byte(k), child...))
}

func (k Key) String(child string) Key {
	return k.Child([]byte(child))
}

func (k Key) Uint8(child uint8) Key {
	return k.Child(Uint8(child))
}

func (k Key) Uint16(child uint16) Key {
	return k.Child(Uint16(child))
}

func (k Key) Uint32(child uint32) Key {
	return k.Child(Uint32(child))
}

func (k Key) Uint64(child uint64) Key {
	return k.Child(Uint64(child))
}

func (k Key) Int8(child int8) Key {
	return k.Child(Int8(child))
}

func (k Key) Int16(child int16) Key {
	return k.Child(Int16(child))
}

func (k Key) Int32(child int32) Key {
	return k.Child(Int32(child))
}

func (k Key) Int64(child int64) Key {
	return k.Child(Int64(child))
}

func (k Key) UUID(child uuid.UUID) Key {
	return k.Child(UUID(child))
}

func (k Key) Equals(other []byte) bool {
	return bytes.Equal(k.Raw(), other)
}

func (k Key) Compare(other []byte) int {
	return bytes.Compare(k.Raw(), other)
}

func (k Key) ParentOf(child []byte) bool {
	return bytes.HasPrefix(child, k)
}

func (k Key) ChildOf(parent []byte) bool {
	return bytes.HasPrefix(k, parent)
}

func String(key string) Key {
	return Key([]byte(key))
}

func UUID(key uuid.UUID) Key {
	return Key(key.Bytes())
}

func Uint8(key uint8) Key {
	return Key(encodeBinary(key))
}

func Uint16(key uint16) Key {
	return Key(encodeBinary(key))
}

func Uint32(key uint32) Key {
	return Key(encodeBinary(key))
}

func Uint64(key uint64) Key {
	return Key(encodeBinary(key))
}

func Int8(key int8) Key {
	return Key(encodeBinary(key))
}

func Int16(key int16) Key {
	return Key(encodeBinary(key))
}

func Int32(key int32) Key {
	return Key(encodeBinary(key))
}

func Int64(key int64) Key {
	return Key(encodeBinary(key))
}

func ParseUint8(raw []byte) (ret uint8, err error) {
	err = decodeBinary(raw, &ret)
	return
}

func ParseUint16(raw []byte) (ret uint16, err error) {
	err = decodeBinary(raw, &ret)
	return
}

func ParseUint32(raw []byte) (ret uint32, err error) {
	err = decodeBinary(raw, &ret)
	return
}

func ParseUint64(raw []byte) (ret uint64, err error) {
	err = decodeBinary(raw, &ret)
	return
}

func ParseInt8(raw []byte) (ret int8, err error) {
	err = decodeBinary(raw, &ret)
	return
}

func ParseInt16(raw []byte) (ret int16, err error) {
	err = decodeBinary(raw, &ret)
	return
}

func ParseInt32(raw []byte) (ret int32, err error) {
	err = decodeBinary(raw, &ret)
	return
}

func ParseInt64(raw []byte) (ret int64, err error) {
	err = decodeBinary(raw, &ret)
	return
}

func encodeBinary(val interface{}) []byte {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, val); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func decodeBinary(val []byte, ptr interface{}) (err error) {
	buf := bytes.NewBuffer(val)
	err = binary.Read(buf, binary.BigEndian, ptr)
	return
}
