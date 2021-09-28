package term

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkopriv2/golang-sdk/ext/go-prompt"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/fatih/color"
	"github.com/pkg/errors"
)

var (
	ErrInterrupt = errors.New("Interrupt")
)

var (
	stringf = fmt.Sprintf
)

// ** READLINE COMPATIBILITY  ** //
var (
	ErrorSymbol    = "✗"
	DefaultInfoMsg = `Sorry, no info available. (Try clearing and using 'tab')`
	PromptFormat   = "$> %v: "
)

type Prompt func(io IO, dest Assign) error

func NewPrompt(msg string, fns ...LineOption) Prompt {
	return func(io IO, dest Assign) error {
		return ReadLine(io, msg, dest, fns...)
	}
}

func ReadPrompt(p Prompt, io IO, a Assign) error {
	return p(io, a)
}

func ReadPrompts(io IO, prmpts []Prompt, assigns ...Assign) (err error) {
	if len(assigns) != len(prmpts) {
		err = errors.Wrapf(errs.ArgError, "Invalid number of assigments")
		return
	}
	for i, p := range prmpts {
		if err = ReadPrompt(p, io, assigns[i]); err != nil {
			return
		}
	}
	return
}

// Returns a default prompt value.
func FmtPrompt(msg string) string {
	return stringf(PromptFormat, msg)
}

// Info accepts the current user input and returns a value
// that is informative or instructive to the user
type Info func(string) string

// Valid accepts the current user data, returning an error
type Check func(string) error

// Complete accepts the current value of the line and returns
// a list of possible completions.
type Complete func(string) []string

// Assign is a function that that accepts the value of the
// line as input - performing some environment side-effect,
// e.g. an assignment and returning whether the operation
// was successful.
//
// It may look strange, but the idea is for the caller to
// supply the necessary context via the assignment function.
// This results in nice function composition without worrying
// about the type of the assignment.
//
// Example:
// ```
//	func SetBytes(outer *[]byte) Assign {
//		return func(inner string) error {
//			*outer = []byte(inner)
//			return nil
//		}
//	}
// ```
type Assign func(string) error

// Matcher matches a string.
type Matcher func(string) bool

// Failure is a standard failure handler.
type Failure func(IO, error)

// ** BASIC IMPLS ** //

func DefaultInfo(in string) string {
	return DefaultInfoMsg
}

func DefaultCheck(string) error {
	return nil
}

func DefaultComplete(string) []string {
	return []string{}
}

func DefaultAssign(string) error {
	return nil
}

// ** UTILITY ASSIGNMENT IMPLS   ** //

func SetBytes(outer *[]byte) Assign {
	return func(inner string) (err error) {
		*outer = []byte(inner)
		return
	}
}

func SetString(outer *string) Assign {
	return func(inner string) (err error) {
		*outer = inner
		return
	}
}

func SetInt(outer *int) Assign {
	return func(inner string) (err error) {
		*outer, err = strconv.Atoi(inner)
		return
	}
}

func SetNone(inner string) (err error) {
	return
}

func SetPointer(raw interface{}) Assign {
	switch ptr := raw.(type) {
	default:
		panic(stringf("Unsupported assignment type: %v", ptr))
	case *string:
		return SetString(ptr)
	case *[]byte:
		return SetBytes(ptr)
	case *int:
		return SetInt(ptr)
	}
}

func ReadDefault(raw interface{}) string {
	switch t := raw.(type) {
	default:
		return ""
	case string:
		return t
	}
}

// ** UTILITY CHECK IMPLS   ** //

func AllOk(checks ...Check) Check {
	return func(cur string) error {
		for _, c := range checks {
			if err := c(cur); err != nil {
				return err
			}
		}
		return nil
	}
}

func OneOk(checks ...Check) Check {
	return func(cur string) error {
		errs := make([]string, 0, len(checks))
		for _, c := range checks {
			if err := c(cur); err != nil {
				errs = append(errs, err.Error())
				continue
			}
			return nil
		}
		return errors.New(fmt.Sprintf("None of the following held: %v", strings.Join(errs, " or ")))
	}
}

func Empty() Check {
	return func(cur string) error {
		if cur != "" {
			return errors.New("Should be empty")
		}
		return nil
	}
}

func NotEmpty() Check {
	return func(cur string) error {
		if cur == "" {
			return errors.New("Should not be empty")
		}
		return nil
	}
}

func NotEndsWith(str string) Check {
	return func(cur string) error {
		if strings.HasSuffix(cur, str) {
			return errors.Errorf("Should not end with [%v]", str)
		}
		return nil
	}
}

func NotBeginsWith(str string) Check {
	return func(cur string) error {
		if strings.HasPrefix(cur, str) {
			return errors.Errorf("Should not begin with [%v]", str)
		}
		return nil
	}
}

func NoSpaces() Check {
	return NotContains(" ")
}

func In(all ...string) Check {
	return func(cur string) error {
		for _, a := range all {
			if a == cur {
				return nil
			}
		}
		return errors.Errorf("Must be one of: %v", all)
	}
}

func Equals(str string) Check {
	return func(cur string) error {
		if cur != str {
			return errors.Errorf("Should equal [%v]", str)
		}
		return nil
	}
}

func NotContains(str string) Check {
	return func(cur string) error {
		if strings.Contains(cur, str) {
			return errors.Errorf("Should not contain [%v]", str)
		}
		return nil
	}
}

func NotMatch(fn func(string) bool) Check {
	return func(cur string) error {
		if fn(cur) {
			return errors.New("Use of that has been reserved.")
		}
		return nil
	}
}

func NotLongerThan(size int) Check {
	return func(cur string) error {
		if len([]rune(cur)) > size {
			return errors.Errorf("Should not be longer than [%v] characters", size)
		}
		return nil
	}
}

func NotShorterThan(size int) Check {
	return func(cur string) error {
		if len([]rune(cur)) < size {
			return errors.Errorf("Should not be shorter than [%v] characters", size)
		}
		return nil
	}
}

func IsMatch(fn func(string) bool, msg string) Check {
	return func(cur string) error {
		if !fn(cur) {
			return errors.Errorf("Format [%v]", msg)
		}
		return nil
	}
}

func IsDuration() Check {
	return func(cur string) error {
		if _, err := time.ParseDuration(strings.Trim(cur, " ")); err != nil {
			return errors.New("Not a duration. Ex: 1y, 1s, 1m, 1y1m1s")
		}
		return nil
	}
}

func InRange(min, max int) Check {
	return func(cur string) error {
		i, err := strconv.Atoi(cur)
		if err != nil {
			return errors.Errorf("Not a number")
		}
		if i < min || i >= max {
			return errors.Errorf("Not in range [%v,%v)", min, max)
		}
		return nil
	}
}

// ** UTILITY COMPLETE IMPLS   ** //

func ColorInfo(info Info, c *color.Color) Info {
	return func(in string) string {
		return c.Sprint(info(in))
	}
}

func PrefixComplete(items ...string) Complete {
	return func(in string) []string {
		return matchPrefix(items, in)
	}
}

func CompleteInfo(fn Complete) Info {
	return func(in string) string {
		return fmt.Sprintf("Must be one of: %v", strings.Join(fn(in), ","))
	}
}

// ** UTILITY INFO IMPLS   ** //

func StaticInfo(msg string) Info {
	return func(in string) string {
		return msg
	}
}

func DisplayError(io IO, err error) {
	fmt.Fprintf(io.StdErr(), "\n\n%v Invalid Input: %v\n\n", ErrorSymbol, err.Error())
}

func DisplayColoredError(io IO, err error) {
	fmt.Fprintf(io.StdErr(), "\n\n%v Invalid Input: %v\n\n", color.RedString(ErrorSymbol), color.YellowString(err.Error()))
}

type LineOption func(*LineOptions)

// Options for integrations with readline.
type LineOptions struct {
	Format   string
	Default  string
	Complete Complete
	Info     Info
	Check    Check
	Retry    bool
	Failed   Failure
	History  LineHistory
}

func BuildLineOptions(opts ...LineOption) (ret LineOptions) {
	ret = LineOptions{PromptFormat, "", DefaultComplete, DefaultInfo, DefaultCheck, false, DisplayColoredError, NullLineHistory{}}
	for _, fn := range opts {
		fn(&ret)
	}
	return
}

func OnError(fn Failure) func(*LineOptions) {
	return func(o *LineOptions) {
		o.Failed = fn
	}
}

func WithAutoRetry() func(*LineOptions) {
	return func(o *LineOptions) {
		o.Retry = true
	}
}

func WithPromptFormat(fmt string) func(*LineOptions) {
	return func(o *LineOptions) {
		o.Retry = true
	}
}

func WithDefault(def string) func(*LineOptions) {
	return func(o *LineOptions) {
		o.Default = def
	}
}

func WithAutoComplete(fn Complete) func(*LineOptions) {
	return func(o *LineOptions) {
		o.Complete = fn
	}
}

func WithAutoInfo(fn Info) func(*LineOptions) {
	return func(o *LineOptions) {
		o.Info = fn
	}
}

func WithAutoCheck(fn Check) func(*LineOptions) {
	return func(o *LineOptions) {
		o.Check = fn
	}
}

func WithCheck(msg string, fn Check) func(*LineOptions) {
	return func(o *LineOptions) {
		o.Check = func(in string) error {
			if err := fn(in); err != nil {
				return errors.New(msg)
			}
			return nil
		}
	}
}

func WithHistoryFile(file string) func(*LineOptions) {
	return func(o *LineOptions) {
		o.History = NewRollingHistory(file)
	}
}

func ReadLine(io IO, prmpt string, assign Assign, fns ...LineOption) (err error) {
	opts := BuildLineOptions(fns...)

	history := opts.History
	if history == nil {
		history = NullLineHistory{}
	}

	// our prompt library doesn't return any indication
	// of error signals.  This is our poor man's way of
	// capturing the SIGINT signal.
	exit := &atomic.Value{}
	exit.Store(false)
	ctrlc := prompt.KeyBind{
		Key: prompt.ControlC,
		Fn: func(b *prompt.Buffer) {
			exit.Store(true)
		},
	}

	complete := func(d prompt.Document) (ret []prompt.Suggest) {
		ret = []prompt.Suggest{}

		all := opts.Complete(d.TextBeforeCursor())
		if all == nil {
			return
		}
		for _, c := range all {
			ret = append(ret, prompt.Suggest{Text: c})
		}
		return
	}

	l, err := history.GetLines()
	if err != nil {
		l = []string{}
	}

	var line string
	for {
		line = prompt.Input(fmt.Sprintf(opts.Format, prmpt), complete,
			prompt.OptionDefault(opts.Default),
			prompt.OptionAddKeyBind(ctrlc),
			prompt.OptionHistory(l))
		if exit.Load().(bool) {
			err = ErrInterrupt
			return
		}
		line = strings.Trim(line, " ")
		if line != "" {
			if len(l) == 0 || l[len(l)-1] != line {
				history.AddLine(line)
			}
		}

		if err = opts.Check(line); err == nil {
			break
		}
		if !opts.Retry {
			return
		}

		opts.Failed(io, err)
	}

	err = assign(line)
	return
}

func filter(all []string, fn func(string) bool) []string {
	ret := make([]string, 0, len(all))
	for _, s := range all {
		if fn(s) {
			ret = append(ret, s)
		}
	}
	return ret
}

func matchPrefix(all []string, prefix string) []string {
	return filter(all, func(s string) bool {
		return strings.HasPrefix(s, prefix)
	})
}
