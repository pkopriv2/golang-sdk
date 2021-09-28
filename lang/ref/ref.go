package ref

import (
	"fmt"
	"path"
	"strings"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/mime"
)

var (
	ErrObject  = errors.New("Enc:Object")
	ErrPointer = errors.New("Enc:Pointer")
	ErrRef     = errors.New("Enc:Ref")
)

// This is a very loose implementation of the JSON pointer schema
// as defined here: https://tools.ietf.org/html/rfc6901
//
// Pointers can refer to elements within a document and can
// contain a reference to the document itself.  This is useful
// where pointers need to reference cross document elements.
//
// Examples:
//
// * #                     <- The entire document
// * #/key1
// * #/key1/key2
// * document.json#/key1
//
type Pointer string

func (p Pointer) Raw() string {
	return string(p)
}

func (p Pointer) Ref() Ref {
	if !strings.Contains(p.Raw(), "#") {
		return "#"
	}

	parts := strings.SplitN(p.Raw(), "#", 2)
	if len(parts) == 1 {
		return Ref("#" + parts[0])
	} else {
		return Ref("#" + parts[1])
	}
}

func (p Pointer) UnsetRef() Pointer {
	if !strings.Contains(p.Raw(), "#") {
		return p
	}

	return Pointer(strings.TrimSuffix(p.Raw(), p.Ref().Raw()))
}

func (p Pointer) Protocol() string {
	if !strings.Contains(p.Raw(), "://") {
		return ""
	}

	return strings.SplitN(p.Raw(), "://", 2)[0]
}

func (p Pointer) SetProtocol(proto string) Pointer {
	var trimmed string
	if !strings.Contains(p.Raw(), "://") {
		trimmed = p.Raw()
	} else {
		trimmed = strings.SplitN(p.Raw(), "://", 2)[1]
	}

	return Pointer(fmt.Sprintf("%v://%v", proto, trimmed))
}

func (p Pointer) Document() (doc string) {
	doc = string(p)
	if strings.Contains(doc, "://") {
		doc = strings.SplitN(doc, "://", 2)[1]
	}
	if strings.Contains(doc, "#") {
		doc = strings.SplitN(doc, "#", 2)[0]
	}
	return
}

func (p Pointer) Mime() string {
	return mime.GetTypeByFilename(p.Document())
}

func (p Pointer) HasExtension(ext string) bool {
	return strings.HasSuffix(p.Document(), ext)
}

func (p Pointer) Extension() string {
	return path.Ext(p.Document())
}

// A ref is a array-like, string representation of a field within
// a document.
//
// This is a very relaxed implementation and generates no errors.
// It is designed to allow for constant strings to be used
// where appropriate.
//
// Additional changes:
//
// * An empty string is the same as the root of an object
// * Empty path elements are ignored
//
// Examples:
//
// * #                     <- The entire document
// * #/key1
// * #/key1/key2
//
type Ref string

func (r Ref) Raw() string {
	return string(r)
}

func (r Ref) Size() int {
	return len(r.Elems())
}

func (r Ref) Empty() bool {
	return r.Size() == 0
}

func (r Ref) Head() string {
	elems := r.Elems()
	if len(elems) == 0 {
		return ""
	}
	return elems[0]
}

func (r Ref) Tail() Ref {
	elems := r.Elems()
	if len(elems) == 0 {
		return "#"
	}
	return Ref(strings.Join(elems[1:], "/"))
}

func (r Ref) Elems() (elems []string) {
	str := strings.Trim(r.Raw(), " ")
	if strings.HasPrefix(str, "#") {
		str = strings.TrimLeft(str, "#")
	}
	if strings.HasPrefix(str, "/") {
		str = strings.TrimLeft(str, "/")
	}
	if str == "" {
		return
	}
	tmp := strings.Split(str, "/")
	for _, elem := range tmp {
		if elem != "" {
			elems = append(elems, elem)
		}
	}
	return
}

func (r Ref) String() string {
	return fmt.Sprintf("#/%v", strings.Join(r.Elems(), "/"))
}
