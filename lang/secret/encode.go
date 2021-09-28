package secret

import "github.com/pkopriv2/golang-sdk/lang/enc"

type EncodableShard struct {
	Shard
}

func (e *EncodableShard) UnmarshalJSON(in []byte) error {
	return enc.ReadIface(enc.Json, in, enc.Impls{Lines.String(): &LineShard{}}, &e.Shard)
}

func (e EncodableShard) MarshalJSON() ([]byte, error) {
	return enc.WriteIface(enc.Json, e.Shard.Type().String(), e.Shard)
}

func EncodeShard(enc enc.Encoder, p Shard) (ret []byte, err error) {
	err = enc.EncodeBinary(&EncodableShard{p}, &ret)
	return
}

func DecodeShard(enc enc.Decoder, in []byte) (ret Shard, err error) {
	tmp := &EncodableShard{}
	err = enc.DecodeBinary(in, &tmp)
	ret = tmp.Shard
	return
}

func ParseShard(enc enc.Decoder, in []byte, ret *Shard) (err error) {
	*ret, err = DecodeShard(enc, in)
	return
}

func WriteShard(enc enc.Encoder, p Shard, in *[]byte) (err error) {
	*in, err = EncodeShard(enc, p)
	return
}
