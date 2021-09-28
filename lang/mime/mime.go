package mime

import (
	"fmt"
	"mime"
	"path"
)

// Adds first class support for common MIME types

const (
	// base types
	None   = "application/none"
	Binary = "binary/octet-stream"
	Json   = "application/json"
	Toml   = "application/toml"
	Yaml   = "application/yaml"
	Pdf    = "application/pdf"

	// file/language types
	Text       = "text/plain"
	Markdown   = "text/markdown"
	Javascript = "text/javascript"
	Html       = "text/html"
	Css        = "text/css"
)

func init() {
	mime.AddExtensionType(".json", Json)
	mime.AddExtensionType(".toml", Toml)
	mime.AddExtensionType(".yaml", Yaml)
	mime.AddExtensionType(".yml", Yaml)
	mime.AddExtensionType(".txt", Text)
	mime.AddExtensionType(".pdf", Pdf)
	mime.AddExtensionType(".bin", Binary)
	mime.AddExtensionType(".md", Markdown)
	mime.AddExtensionType(".js", Javascript)
	mime.AddExtensionType(".html", Html)
	mime.AddExtensionType(".css", Css)
}

func GetTypeByAlias(alias string) string {
	switch alias {
	default:
		panic(fmt.Sprintf("Unknown type [%v]", alias))
	case Text:
		return mime.TypeByExtension(".txt")
	case Markdown:
		return mime.TypeByExtension(".md")
	case Json:
		return mime.TypeByExtension(".json")
	case Toml:
		return mime.TypeByExtension(".toml")
	case Yaml:
		return mime.TypeByExtension(".yaml")
	case Pdf:
		return mime.TypeByExtension(".pdf")
	case Binary:
		return mime.TypeByExtension(".bin")
	}
}

func GetTypeByFilename(file string) string {
	return GetTypeByExtension(path.Ext(file))
}

func GetTypeByExtension(ext string) string {
	t, _, _ := mime.ParseMediaType(mime.TypeByExtension(ext))
	return t
}
