package term

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"runtime"
	"strings"

	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

func MustFormat(s string) Format {
	switch strings.ToLower(s) {
	default:
		panic(fmt.Sprintf("Invalid format [%v]", s))
	case "json":
		return Json
	case "toml":
		return Toml
	case "yaml":
		return Yaml
	case "text":
		return Text
	}
}

var (
	Json Format = json{}
	Yaml Format = yaml{}
	Toml Format = toml{}
	Text Format = text{}
)

type Format interface {
	Ext() string
	Encoder() enc.EncoderDecoder
}

type json struct{}

func (j json) Ext() string {
	return "json"
}

func (j json) Encoder() enc.EncoderDecoder {
	return enc.Json
}

type yaml struct{}

func (y yaml) Ext() string {
	return "yaml"
}

func (y yaml) Encoder() enc.EncoderDecoder {
	return enc.Yaml
}

type toml struct{}

func (y toml) Ext() string {
	return "toml"
}

func (y toml) Encoder() enc.EncoderDecoder {
	return enc.Toml
}

type text struct{}

func (t text) Ext() string {
	return "txt"
}

func (t text) Encoder() enc.EncoderDecoder {
	return enc.Text
}

var SystemEditor Editor = func() Editor {
	if editor := os.Getenv("EDITOR"); editor != "" {
		return FileEditor(editor)
	}
	switch runtime.GOOS {
	case "windows":
		return Notepad
	default:
		return Vim
	}
}()

// An Editor is capable of displaying and updating a value.
type Editor interface {
	View(f Format, val interface{}) error
	Edit(f Format, cur, new interface{}) error
}

const (
	Vim     FileEditor = "vim"
	Nano    FileEditor = "nano"
	Emacs   FileEditor = "emacs"
	Notepad FileEditor = "notepad.exe"
)

type EditOptions struct {
	Editor Editor
	Format Format
}

func WithFormat(fmt Format) func(*EditOptions) {
	return func(o *EditOptions) {
		o.Format = fmt
	}
}

func WithEditor(editor Editor) func(*EditOptions) {
	return func(o *EditOptions) {
		o.Editor = editor
	}
}

func buildEditOptions(fns ...func(*EditOptions)) EditOptions {
	def := EditOptions{Editor: SystemEditor, Format: Toml}
	for _, fn := range fns {
		fn(&def)
	}
	return def
}

func Edit(val, ptr interface{}, fns ...func(*EditOptions)) error {
	opts := buildEditOptions(fns...)
	return opts.Editor.Edit(opts.Format, val, ptr)
}

func View(val interface{}, fns ...func(*EditOptions)) error {
	opts := buildEditOptions(fns...)
	return opts.Editor.View(opts.Format, val)
}

// ** EDITING INTERFACES  ** //

// A file editor calls out to the system to open
type FileEditor string

func (e FileEditor) Cmd() string {
	return string(e)
}

func (f FileEditor) View(fmt Format, val interface{}) (err error) {
	if src, ok := val.([]byte); ok {
		return ViewTmpFile(fmt, src, func(path string) error {
			return Exec(SystemShell, SystemIO, string(f), WithArgs(path))
		})
	}

	var src []byte
	if err = fmt.Encoder().EncodeBinary(val, &src); err != nil {
		return
	}
	return ViewTmpFile(fmt, src, func(path string) error {
		return Exec(SystemShell, SystemIO, string(f), WithArgs(path))
	})
}

func (f FileEditor) Edit(fmt Format, cur interface{}, new interface{}) (err error) {
	if src, ok := cur.([]byte); ok {
		if dst, ok := new.(*[]byte); ok {
			return EditTmpFile(fmt, src, dst, func(path string) error {
				return Exec(SystemShell, SystemIO, string(f), WithArgs(path))
			})
		}
		return errors.Errorf("Invalid destination [%v]", reflect.TypeOf(new))
	}

	var src []byte
	if err = fmt.Encoder().EncodeBinary(cur, &src); err != nil {
		return
	}

	var dst []byte
	if err = EditTmpFile(fmt, src, &dst, func(path string) error {
		return Exec(SystemShell, SystemIO, string(f), WithArgs(path))
	}); err != nil {
		return
	}

	err = fmt.Encoder().DecodeBinary(dst, new)
	return
}

func EditTmpFile(f Format, cur []byte, new *[]byte, fn func(string) error) (err error) {
	dir := path.Join(os.TempDir(), "termx")
	if err = os.MkdirAll(dir, 0777); err != nil {
		return
	}

	file := path.Join(dir, fmt.Sprintf("%v.%v", uuid.NewV1(), f.Ext()))
	if err = ioutil.WriteFile(file, cur, 0600); err != nil {
		return
	}
	defer os.Remove(file)
	if err = fn(file); err != nil {
		return
	}

	*new, err = ioutil.ReadFile(file)
	return
}

func ViewTmpFile(f Format, cur []byte, fn func(string) error) (err error) {
	dir := path.Join(os.TempDir(), "termx")
	if err = os.MkdirAll(dir, 0777); err != nil {
		return
	}

	file := path.Join(dir, fmt.Sprintf("%v.%v", uuid.NewV1(), f.Ext()))
	if err = ioutil.WriteFile(file, cur, 0600); err != nil {
		return
	}
	defer os.Remove(file)

	err = fn(file)
	return
}
