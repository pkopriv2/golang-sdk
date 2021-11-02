package rpc

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/concurrent"
	"github.com/pkopriv2/golang-sdk/lang/errs"
)

type header struct {
	Id   uint64 // 8-bytes
	Size uint32 // 4-bytes
}

func (h header) ReadPayload(r io.Reader, ptr *[]byte) (err error) {
	*ptr = make([]byte, h.Size)
	_, err = io.ReadFull(r, *ptr)
	return
}

// Reads a packet header from the input reader.
func readHeaderRaw(r io.Reader, h *header) (err error) {
	err = binary.Read(r, binary.BigEndian, h)
	return
}

// Writes a packet header to the writer.
func writeHeaderRaw(w io.Writer, h header) (err error) {
	err = binary.Write(w, binary.BigEndian, h)
	return
}

// The base packet structure.
type packet struct {
	Header header
	Data   []byte
}

var (
	ids = concurrent.NewAtomicCounter()
)

func newPacket(data []byte) (ret packet) {
	ret = packet{
		Header: header{
			Id:   ids.Inc(),
			Size: uint32(len(data))},
		Data: data}
	return
}

// Reads a packet from the reader.
func readPacketRaw(r io.Reader) (p packet, err error) {
	if err = readHeaderRaw(r, &p.Header); err != nil {
		return
	}

	err = p.Header.ReadPayload(r, &p.Data)
	return
}

// Writes a packet to the writer.  In order to facilitate shared writers,
// the packet is first serialized, then written in a single write call.
func writePacketRaw(w io.Writer, p packet) (err error) {
	if p.Header.Size != uint32(len(p.Data)) {
		err = errors.Wrapf(errs.ArgError, "Invalid packet.  Header size does not match payload [%v]", p)
		return
	}

	buf := &bytes.Buffer{}
	if err = writeHeaderRaw(buf, p.Header); err != nil {
		return
	}
	if _, err = buf.Write(p.Data); err != nil {
		return
	}

	_, err = w.Write(buf.Bytes())
	return
}
