package msgs

import (
	"bytes"
	html "html/template"
	text "text/template"

	"github.com/pkopriv2/golang-sdk/lang/markdown"
)

// This package defines a universal messaging abstraction
// that can be used as the basis for communicating events
// through verified channels. (e.g. email, sms, etc..)

const (
	Version = "builder-0.0.1"
)

type TemplateBuilder func(*Template)

type Message struct {
	Template
	Recipient string
	Bindings  interface{}
}

func Compile(t Template, to string, data interface{}) Message {
	return Message{t, to, data}
}

type Template struct {
	Subject string
	Short   *text.Template
	Plain   *text.Template
	Html    *html.Template
}

func BuildTemplate(sub string, builders ...TemplateBuilder) (ret Template) {
	ret = Template{Subject: sub}
	for _, fn := range builders {
		fn(&ret)
	}
	return
}

func (m Template) RenderShort(data interface{}) (ret []byte, err error) {
	buf := &bytes.Buffer{}
	err = m.Short.Execute(buf, data)
	ret = buf.Bytes()
	return
}

func (m Template) RenderText(data interface{}) (ret []byte, err error) {
	buf := &bytes.Buffer{}
	err = m.Plain.Execute(buf, data)
	ret = buf.Bytes()
	return
}

func (m Template) RenderHtml(data interface{}) (ret []byte, err error) {
	buf := &bytes.Buffer{}
	err = m.Html.Execute(buf, data)
	ret = buf.Bytes()
	return
}

func AsMicro(s string) TemplateBuilder {
	var err error
	return func(m *Template) {
		m.Short, err = text.New(Version).Parse(s)
		if err != nil {
			panic(err)
		}
	}
}

func AsText(s string) TemplateBuilder {
	var err error
	return func(m *Template) {
		m.Plain, err = text.New(Version).Parse(s)
		if err != nil {
			panic(err)
		}
	}
}

func AsHtml(s string) TemplateBuilder {
	var err error
	return func(m *Template) {
		m.Html, err = html.New(Version).Parse(s)
		if err != nil {
			panic(err)
		}
	}
}

// overrides other html
func AsMarkdown(md string) TemplateBuilder {
	return AsHtml(markdown.Render(md))
}
