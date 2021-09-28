package term

import (
	"io"
	"io/ioutil"
	"os"
)

var SystemIO = IO{
	os.Stdin,
	os.Stdout,
	os.Stderr}

var DevNull = IO{
	os.Stdin,
	ioutil.Discard,
	ioutil.Discard}

// The standard input/output abstraction - designed to mirror standard systems
type IO struct {
	In  io.Reader
	Out io.Writer
	Err io.Writer
}

func (t IO) StdIn() io.Reader {
	return t.In
}

func (t IO) StdOut() io.Writer {
	return t.Out
}

func (t IO) StdErr() io.Writer {
	return t.Err
}

func (t IO) SwapIn(in io.Reader) IO {
	return IO{in, t.Out, t.Err}
}

func (t IO) SwapOut(out io.Writer) IO {
	return IO{t.In, out, t.Err}
}

func (t IO) SwapErr(err io.Writer) IO {
	return IO{t.In, t.Out, err}
}
