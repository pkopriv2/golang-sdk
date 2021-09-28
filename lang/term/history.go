package term

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkg/errors"
	filelock "github.com/zbiljic/go-filelock"
)

// LineHistory manages a collection of commands that have
// historically been entered.  Rolling history and managing
// the number of lines stored is implementation dependent.
type LineHistory interface {

	// Adds a line to the history file .  The added line is guaranteed
	// to be returned as the "first" item in a subsequent #GetLines()
	// call - assuming no other lines have been added.
	AddLine(line string) error

	// Returns all the lines currently in the history - sorted in reverse
	// order relative to the time they were inserted.
	GetLines() ([]string, error)
}

// Implements a line history that stores NOTHING.  This is useful
// for prompts that may contain sensitive data.
type NullLineHistory struct{}

func (n NullLineHistory) AddLine(line string) (err error) {
	return
}

func (n NullLineHistory) GetLines() (ret []string, err error) {
	return
}

type RollingLineHistory struct {
	file string
}

func NewRollingHistory(file string) LineHistory {
	return &RollingLineHistory{file}
}

func (r *RollingLineHistory) ensureDir() (err error) {
	dir := filepath.Dir(r.file)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.Mkdir(dir, 0700)
	}
	return
}

func (r *RollingLineHistory) AddLine(line string) (err error) {
	if err = r.ensureDir(); err != nil {
		return
	}

	f, err := os.OpenFile(r.file, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	err = historyAdd(f, line)
	return
}

func (r *RollingLineHistory) GetLines() (lines []string, err error) {
	if err = r.ensureDir(); err != nil {
		return
	}

	f, err := os.Open(r.file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	_, lines, err = readHistoryFile(f)
	return lines, err

}

func historyAdd(f *os.File, line string) (err error) {
	abs, err := filepath.Abs(f.Name())
	if err != nil {
		return
	}

	fl, err := filelock.New(abs + ".lock")
	if err != nil {
		return
	}

	if err = fl.Lock(); err != nil {
		return
	}
	defer fl.Unlock()

	max, lines, err := readHistoryFile(f)
	if err != nil {
		max, lines = 500, []string{}
	}

	if _, err = f.Seek(0, 0); err != nil {
		return
	}
	if err = f.Truncate(0); err != nil {
		return
	}

	if len(lines) >= max {
		lines = lines[1:max]
	}

	err = writeHistoryFile(f, max, append(lines, line))
	return
}

func readHistoryFile(f *os.File) (max int, lines []string, err error) {
	raw, err := ioutil.ReadAll(f)
	if err != nil {
		return
	}

	lines = strings.Split(string(raw), "\n")
	if len(lines) < 1 {
		err = errors.Wrapf(errs.StateError, "Invalid history file [%v]", f.Name())
		return
	}

	max, err = strconv.Atoi(lines[0])
	if err != nil {
		err = errors.Wrapf(err, "Error reading max items from file [%v]", f.Name())
		return
	}

	lines = lines[1:]
	return
}

func writeHistoryFile(f *os.File, max int, lines []string) (err error) {
	_, err = f.WriteString(strings.Join(append([]string{strconv.Itoa(max)}, lines...), "\n"))
	return
}
