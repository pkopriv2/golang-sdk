package context

import (
	"fmt"
	"io"
	"strings"

	"github.com/pkg/errors"
)

var (
	ErrLogLevel = errors.New("Context:LogLevel")
)

const (
	Off LogLevel = iota
	Error
	Info
	Debug
)

func ParseLogLevel(l string) (LogLevel, error) {
	switch strings.ToLower(l) {
	default:
		return Off, errors.Wrapf(ErrLogLevel, "Invalid logging level [%v].  Must be one of [Off, Debug, Info, Error]", l)
	case "off":
		return Off, nil
	case "error":
		return Error, nil
	case "info":
		return Info, nil
	case "debug":
		return Debug, nil
	}
}

type LogLevel int

func (l LogLevel) String() string {
	switch l {
	default:
		return "Unknown"
	case Off:
		return "Off"
	case Error:
		return "Error"
	case Info:
		return "Info"
	case Debug:
		return "Debug"
	}
}

type Logger interface {
	Fmt(string, ...interface{}) Logger
	Debug(string, ...interface{})
	Info(string, ...interface{})
	Error(string, ...interface{})
}

type logger struct {
	out  io.Writer
	lvl  LogLevel
	fmt  string
	args []interface{}
}

func NewLogger(out io.Writer, lvl LogLevel, fmt string, args ...interface{}) Logger {
	return &logger{out, lvl, fmt, args}
}

func (l *logger) println(format string, args ...interface{}) {
	fmt.Fprintln(l.out,
		fmt.Sprintf("%v: %v",
			fmt.Sprintf(l.fmt, l.args...),
			fmt.Sprintf(format, args...)))
}

func (l *logger) Fmt(format string, args ...interface{}) Logger {
	return NewLogger(l.out, l.lvl, fmt.Sprintf("%v: %v", fmt.Sprintf(l.fmt, l.args...), fmt.Sprintf(format, args...)))
}

func (s *logger) Debug(format string, args ...interface{}) {
	if s.lvl >= Debug {
		s.println(format, args...)
	}
}

func (s *logger) Info(format string, args ...interface{}) {
	if s.lvl >= Info {
		s.println(format, args...)
	}
}

func (s *logger) Error(format string, args ...interface{}) {
	if s.lvl >= Error {
		s.println(format, args...)
	}
}
