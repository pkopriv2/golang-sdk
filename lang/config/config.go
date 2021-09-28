package config

import (
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkopriv2/golang-sdk/lang/path"
	"github.com/spf13/afero"
)

var (
	Errconfig     = errors.New("Config:Error")
	ErrMissingKey = errors.New("Config:MissingKey")
)

func WriteConfig(c Config, file string, mode os.FileMode) (err error) {
	var e enc.Encoder
	switch {
	case strings.HasSuffix(file, ".yaml"):
		e = enc.Yaml
	case strings.HasSuffix(file, ".json"):
		e = enc.Json
	case strings.HasSuffix(file, ".toml"):
		e = enc.Toml
	}

	file, err = path.Expand(file)
	if err != nil {
		return
	}

	bytes, err := enc.Encode(e, c)
	if err != nil {
		return
	}

	fs, dir := afero.NewOsFs(), path.Dir(file)

	exists, err := afero.Exists(fs, dir)
	if err != nil {
		return
	}

	if !exists {
		if err = fs.MkdirAll(dir, 0755); err != nil {
			return
		}
	}

	err = afero.WriteFile(fs, file, bytes, mode)
	return
}

func ParseFromFile(file string) (ret Config, err error) {
	var dec enc.Decoder
	switch {
	case strings.HasSuffix(file, ".yaml"):
		dec = enc.Yaml
	case strings.HasSuffix(file, ".json"):
		dec = enc.Json
	case strings.HasSuffix(file, ".toml"):
		dec = enc.Toml
	}

	file, err = path.Expand(file)
	if err != nil {
		return
	}

	f, err := os.Open(file)
	if err != nil {
		return
	}
	defer f.Close()
	return Parse(dec, f)
}

func Parse(dec enc.Decoder, r io.Reader) (ret Config, err error) {
	raw, err := ioutil.ReadAll(r)
	if err != nil {
		return
	}

	err = dec.DecodeBinary(raw, &ret)
	return
}

type Config map[string]string

func NewConfig() Config {
	return Config(make(map[string]string))
}

func (c Config) Get(name string, dec Decoder, ptr interface{}) (ok bool, err error) {
	str, ok := c[name]
	if !ok {
		return
	}

	if err = dec(str, ptr); err == nil {
		return
	}

	err = errors.Wrapf(err, "Error parsing config [%v]", name)
	return
}

func (c Config) GetOrDefault(name string, dec Decoder, ptr, def interface{}) (err error) {
	ok, err := c.Get(name, dec, ptr)
	if err != nil || ok {
		return
	}

	refPtr, defVal :=
		reflect.ValueOf(ptr),
		reflect.ValueOf(def)
	if refPtr.Kind() != reflect.Ptr {
		err = errors.Wrapf(errs.ArgError, "Expected a pointer [%v]", ptr)
		return
	}

	refPtr.Elem().Set(defVal)
	return
}

func (c Config) Require(name string, dec Decoder, ptr interface{}) (ok bool, err error) {
	ok, err = c.Get(name, dec, ptr)
	if err != nil {
		return
	}

	if !ok {
		err = errors.Wrapf(errs.StateError, "Missing required config [%v]", name)
	}
	return
}
