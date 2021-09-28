package errs

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/pkg/errors"
)

var (
	ArgError      = fmt.Errorf("Errs:ArgError")
	StateError    = fmt.Errorf("Errs:StateError")
	ClosedError   = fmt.Errorf("Errs:ClosedError")
	CanceledError = errors.New("Errs:CanceledError")
)

func Or(all ...error) error {
	for _, e := range all {
		if e != nil {
			return e
		}
	}
	return nil
}

func Is(actual error, match ...error) bool {
	err := Extract(actual, match...)
	return err != nil
}

func Extract(err error, match ...error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	for _, cur := range match {
		if strings.Contains(msg, cur.Error()) {
			return cur
		}
	}
	return nil
}

func NotZero(a interface{}) error {
	if reflect.Zero(reflect.TypeOf(a)).Interface() == a {
		return errors.Wrapf(ArgError, "Unexpected zero value [%v]", a)
	}
	return nil
}

func NotNil(a interface{}) error {
	if reflect.ValueOf(a).IsNil() {
		return errors.Wrapf(ArgError, "Unexpected nil value [%v]", a)
	}
	return nil
}
