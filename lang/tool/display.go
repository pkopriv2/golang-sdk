package tool

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"text/template"
	"time"
	"unicode"

	"github.com/fatih/color"
	"github.com/leekchan/accounting"
	colorable "github.com/mattn/go-colorable"
	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/crypto"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkopriv2/golang-sdk/lang/term"
	uuid "github.com/satori/go.uuid"
)

const (
	OkMark     = "✓"
	InfoMark   = "!"
	ErrorMark  = "✗"
	NoticeMark = "!*"
)

var (
	MarkData = struct {
		Ok     string
		Info   string
		Error  string
		Notice string
	}{
		OkMark, InfoMark, ErrorMark, NoticeMark,
	}
)

const (
	Byte     = 1
	KiloByte = Byte << 10
	MegaByte = KiloByte << 10
	GigaByte = MegaByte << 10
)

const (
	TimeFormat = time.StampMilli
	DateFormat = time.Stamp
	DayFormat  = "1/2/2006"
)

type Formatter func(string) string

func NoFormat(in string) string {
	return in
}

func ColoredError(in string) string {
	return color.RedString(in)
}

func ColoredInfo(in string) string {
	return color.YellowString(in)
}

func ColoredOk(in string) string {
	return color.GreenString(in)
}

func ColoredNotice(in string) string {
	return color.Set(color.Attribute(38), color.Attribute(5), color.Attribute(243)).Sprint(in)
}

func ColoredHeader(in string) string {
	return color.Set(color.Attribute(38), color.Attribute(5), color.Attribute(74)).Sprint(in)
}

func ColoredItem(in string) string {
	return color.MagentaString(in)
}

func ColoredMark(in string) string {
	switch in {
	default:
		return in
	case OkMark:
		return color.GreenString(in)
	case InfoMark:
		return color.YellowString(in)
	case ErrorMark:
		return color.RedString(in)
	}
}

// Adapted from: https://github.com/cloudfoundry/bytefmt/blob/fb0928873a0334aee5c6e02e2ba435357e4296fb/bytes.go
func ByteSizeFormatter(bytes int) string {
	unit := ""
	value := float32(bytes)

	switch {
	case bytes >= GigaByte:
		unit = "G"
		value = value / GigaByte
	case bytes >= MegaByte:
		unit = "M"
		value = value / MegaByte
	case bytes >= KiloByte:
		unit = "K"
		value = value / KiloByte
	case bytes >= Byte:
		unit = "B"
	case bytes == 0:
		return "0"
	}

	stringValue := strings.TrimSuffix(fmt.Sprintf("%.1f", value), ".0")
	return fmt.Sprintf("%s%s", stringValue, unit)
}

func BytesFormatter(bin []byte) string {
	if len(bin) == 0 {
		return "empty"
	}
	idx := 8
	if len(bin) < 8 {
		idx = len(bin)
	}
	return crypto.Bytes(bin).Base64()[:idx]
}

func UUIDFormatter(id uuid.UUID) string {
	return id.String()[:8]
}

func DurationFormatter(dur time.Duration) (ret string) {
	dur = dur - (dur % time.Second)
	if dur > 1*time.Hour {
		dur = dur - (dur % time.Minute)
	}
	if dur > 24*time.Hour {
		dur = dur - (dur % time.Hour)
	}
	if dur > 3*24*time.Hour {
		return fmt.Sprintf("%vd", int((dur-(dur%24*time.Hour))/(24*time.Hour)))
	}

	ret = dur.String()
	if strings.HasSuffix(ret, "m0s") {
		ret = ret[:len(ret)-2]
	}
	if strings.HasSuffix(ret, "h0m") {
		ret = ret[:len(ret)-2]
	}
	return
}

func TimeFormatter(date time.Time) string {
	return date.Local().Format(TimeFormat)
}

func DateFormatter(date time.Time) string {
	return date.Local().Format(DateFormat) // FIXME: Determine formatting! (possibly pull from environment?)
}

func DayFormatter(date time.Time) string {
	return date.Local().Format(DayFormat)
}

func MoneyFormatter(units int64) string {
	ac := accounting.Accounting{Symbol: "$", Precision: 2}
	return ac.FormatMoney(float64(units) / float64(100))
}

func CentsFormatter(units int) string {
	return MoneyFormatter(int64(units))
}

func ColumnFormatter(max int, val string) string {
	val = strings.Replace(val, "\n", "\\n", -1)

	raw := []rune(val)
	if len([]rune(val)) > max {
		return string(raw[:max-3]) + "..."
	}
	pad := make([]rune, 0, max-len(raw))
	for i := len(raw); i < max; i++ {
		pad = append(pad, ' ')
	}
	return string(append(raw, pad...))
}

func SinceFormatter(then time.Time) string {
	return DurationFormatter(time.Now().Sub(then))
}

func NewDateUntilFormatter(now time.Time) interface{} {
	return func(then time.Time) string {
		return DurationFormatter(time.Now().Sub(now))
	}
}

func BoolFormatter(ok bool) string {
	if ok {
		return OkMark
	} else {
		return ErrorMark
	}
}

func TagsFormatter(tags []string) string {
	return "[" + strings.Join(tags, ",") + "]"
}

// Taken and modified from:
// https://raw.githubusercontent.com/mitchellh/go-wordwrap/master/wordwrap.go
func WrapFormatter(lim int, s string) interface{} {

	// Initialize a buffer with a slightly larger size to account for breaks
	init := make([]byte, 0, len(s))
	buf := bytes.NewBuffer(init)

	var current int
	var wordBuf, spaceBuf bytes.Buffer

	for _, char := range s {
		if char == '\n' {
			if wordBuf.Len() == 0 {
				if current+spaceBuf.Len() > lim {
					current = 0
				} else {
					current += spaceBuf.Len()
					spaceBuf.WriteTo(buf)
				}
				spaceBuf.Reset()
			} else {
				current += spaceBuf.Len() + wordBuf.Len()
				spaceBuf.WriteTo(buf)
				spaceBuf.Reset()
				wordBuf.WriteTo(buf)
				wordBuf.Reset()
			}
			buf.WriteRune(char)
			current = 0
		} else if unicode.IsSpace(char) {
			if spaceBuf.Len() == 0 || wordBuf.Len() > 0 {
				current += spaceBuf.Len() + wordBuf.Len()
				spaceBuf.WriteTo(buf)
				spaceBuf.Reset()
				wordBuf.WriteTo(buf)
				wordBuf.Reset()
			}

			spaceBuf.WriteRune(char)
		} else {

			wordBuf.WriteRune(char)

			if current+spaceBuf.Len()+wordBuf.Len() > lim && wordBuf.Len() < lim {
				buf.WriteRune('\n')
				current = 0
				spaceBuf.Reset()
			}
		}
	}

	if wordBuf.Len() == 0 {
		if current+spaceBuf.Len() <= lim {
			spaceBuf.WriteTo(buf)
		}
	} else {
		spaceBuf.WriteTo(buf)
		wordBuf.WriteTo(buf)
	}

	return buf.String()
}

var (
	DefaultFuncs = map[string]interface{}{
		"ok":     ColoredOk,
		"info":   ColoredInfo,
		"error":  ColoredError,
		"item":   ColoredItem,
		"mark":   ColoredMark,
		"notice": ColoredNotice,
		"header": ColoredHeader,
		"field":  NoFormat,
		"org":    NoFormat,
		"bool":   BoolFormatter,
		"bin":    BytesFormatter,
		"kb":     ByteSizeFormatter,
		"date":   DateFormatter,
		"time":   TimeFormatter,
		"since":  SinceFormatter,
		"day":    DayFormatter,
		"dur":    DurationFormatter,
		"col":    ColumnFormatter,
		"uuid":   UUIDFormatter,
		"money":  MoneyFormatter,
		"cents":  CentsFormatter,
		"tags":   TagsFormatter,
		"wrap":   WrapFormatter,
	}

	ColoredFuncs = map[string]interface{}{
		"ok":     ColoredOk,
		"info":   ColoredInfo,
		"error":  ColoredError,
		"item":   ColoredItem,
		"mark":   ColoredMark,
		"notice": ColoredNotice,
		"header": ColoredHeader,
	}
)

type DisplayOption func(*DisplayOptions)

type DisplayOptions struct {
	Funcs map[string]interface{}
	Data  interface{}
}

func BuildDisplayOptions(fns ...DisplayOption) DisplayOptions {
	funcs := make(map[string]interface{})
	for k, v := range DefaultFuncs {
		funcs[k] = v
	}

	opts := DisplayOptions{Funcs: funcs}
	for _, o := range fns {
		o(&opts)
	}
	return opts
}

func WithColor() DisplayOption {
	return func(d *DisplayOptions) {
		for name, fn := range ColoredFuncs {
			d.Funcs[name] = fn
		}
	}
}

func WithOrgAliases(aliases map[string]string) DisplayOption {
	return func(d *DisplayOptions) {
		d.Funcs["org"] = func(in string) string {
			if a, ok := aliases[in]; ok {
				return a
			} else {
				return in
			}
		}
	}
}

func WithFunc(name string, fn interface{}) DisplayOption {
	return func(d *DisplayOptions) {
		d.Funcs[name] = fn
	}
}

func WithData(data interface{}) DisplayOption {
	return func(d *DisplayOptions) {
		d.Data = data
	}
}

func DisplayIO(env Environment, out *os.File, templ string, fns ...DisplayOption) (err error) {
	opts := BuildDisplayOptions(fns...)

	compiled, err := Compile(env, templ, opts.Funcs)
	if err != nil {
		return
	}

	err = compiled.Execute(colorable.NewColorable(out), opts.Data)
	return
}

func DisplayStdOut(env Environment, templ string, fns ...DisplayOption) (err error) {
	err = DisplayIO(env, os.Stdout, templ, fns...)
	return
}

func DisplayStdErr(env Environment, templ string, fns ...DisplayOption) (err error) {
	err = DisplayIO(env, os.Stderr, templ, fns...)
	return
}

func Compile(env Environment, templ string, fns map[string]interface{}) (ret *template.Template, err error) {
	ret, err = template.New("root").Funcs(fns).Parse(templ)
	return
}

// A couple standard templates (Just errors, and notices and such)
var (
	failureTemplate = `
{{ .Mark | mark }} {{.Msg}}!

`

	noticeTemplate = `{{ .Msg | notice }}
`
)

func DisplayUnauthorized(env Environment) error {
	return DisplayFailure(env, errors.New("Insufficient permissions"))
}

func DisplayFailure(env Environment, err error) error {
	return DisplayStdErr(env, failureTemplate, WithData(struct {
		Mark string
		Msg  string
	}{
		ErrorMark,
		err.Error(),
	}))
}

func DisplayNotice(env Environment, str string, args ...interface{}) error {
	return DisplayStdErr(env, noticeTemplate, WithData(struct {
		Notice string
		Msg    string
	}{
		NoticeMark,
		fmt.Sprintf(str, args...),
	}))
}

func NewConfirmationPrompt(env Environment, msg string, opts ...term.LineOption) term.Prompt {
	return term.NewPrompt(
		msg,
		append(
			[]term.LineOption{
				term.OnError(term.DisplayColoredError),
				term.WithDefault("yes"),
				term.WithAutoCheck(
					term.In("yes", "no")),
				term.WithAutoComplete(
					term.PrefixComplete("yes", "no"),
				)},
			opts...)...,
	)
}

func Confirm(env Environment, msg string, opts ...term.LineOption) (err error) {
	var yesOrNo string
	if err = term.ReadPrompt(
		NewConfirmationPrompt(env, msg, opts...), env.Terminal.IO, term.SetString(&yesOrNo)); err != nil {
		return
	}

	if yesOrNo != "yes" {
		err = errors.Wrap(errs.CanceledError, "User Canceled")
		return
	}
	return
}

func Step(env Environment, prmpt string, fn func() error) (err error) {
	info := func(str string, args ...interface{}) string {
		return fmt.Sprintf("%v ", fmt.Sprintf(str, args...))
	}

	done := func(str string, args ...interface{}) string {
		return fmt.Sprintf("{{.Ok | mark }}\n")
	}

	fail := func(err error) string {
		return fmt.Sprintf("{{.Error | mark }}\n")
	}

	if err = DisplayStdOut(env, info(prmpt)); err != nil {
		return
	}

	if err = fn(); err != nil {
		DisplayStdOut(env, fail(err), WithData(MarkData))
		return
	}

	DisplayStdOut(env, done(""), WithData(MarkData))
	return
}
