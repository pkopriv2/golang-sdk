package term

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/pkg/errors"
)

var SystemShell Shell = func() Shell {
	switch runtime.GOOS {
	default:
		return Bash
	case "windows":
		return Powershell
	}
}()

var SystemLang string = func() string {
	switch runtime.GOOS {
	default:
		return "bash"
	case "windows":
		return "powershell"
	}
}()

func ParseShell(lang string) (ret Shell, err error) {
	switch strings.ToLower(lang) {
	default:
		err = errors.Errorf("Unsupported shell [%v]", lang)
	case "none":
		ret = Native
	case "bash":
		ret = Bash
	case "zsh":
		ret = Zsh
	case "powershell":
		ret = Powershell
	}
	return
}

var (
	Native     Shell = native{}
	Bash       Shell = bash{}
	Zsh        Shell = zsh{}
	Powershell Shell = powershell{}
)

func Exec(sh Shell, io IO, cmd string, fns ...func(*ShellOptions)) (err error) {
	proc, err := sh.Start(io, cmd, fns...)
	if err != nil {
		return
	}
	err = proc.Wait()
	return
}

// A shell runs scripts on the host machine.
type Shell interface {
	Start(io IO, cmd string, fns ...func(*ShellOptions)) (*exec.Cmd, error)
}

type ShellOptions struct {
	Env  map[string]string
	Args []string
}

func WithEnv(env map[string]string) func(*ShellOptions) {
	return func(o *ShellOptions) {
		o.Env = env
	}
}

func WithArgs(args ...string) func(*ShellOptions) {
	return func(o *ShellOptions) {
		o.Args = args
	}
}

func buildShellOptions(fns []func(*ShellOptions)) (ret ShellOptions) {
	ret = ShellOptions{Env: map[string]string{}}
	for _, fn := range fns {
		fn(&ret)
	}
	return
}

type bash struct{}

func (b bash) Start(io IO, cmd string, fns ...func(*ShellOptions)) (proc *exec.Cmd, err error) {
	opts := buildShellOptions(fns)

	proc = exec.Command("/bin/bash", "-c", strings.Join(append([]string{cmd}, opts.Args...), " "))
	proc.Env = append(os.Environ(), FlattenEnv(opts.Env)...)
	proc.Stdin = io.StdIn()
	proc.Stdout = io.StdOut()
	proc.Stderr = io.StdErr()
	err = proc.Start()
	return
}

type powershell struct{}

func (b powershell) Start(io IO, cmd string, fns ...func(*ShellOptions)) (proc *exec.Cmd, err error) {
	opts := buildShellOptions(fns)

	proc = exec.Command("powershell.exe", "-Command", "-")
	proc.Env = append(os.Environ(), FlattenEnv(opts.Env)...)
	proc.Stdin = bytes.NewBufferString(cmd)
	proc.Stdout = io.StdOut()
	proc.Stderr = io.StdErr()
	err = proc.Start()
	return
}

type zsh struct{}

func (b zsh) Start(io IO, cmd string, fns ...func(*ShellOptions)) (proc *exec.Cmd, err error) {
	opts := buildShellOptions(fns)

	proc = exec.Command("/bin/zsh", "-c", strings.Join(append([]string{cmd}, opts.Args...), " "))
	proc.Env = append(os.Environ(), FlattenEnv(opts.Env)...)
	proc.Stdin = io.StdIn()
	proc.Stdout = io.StdOut()
	proc.Stderr = io.StdErr()
	err = proc.Start()
	return
}

type native struct{}

func (b native) Start(io IO, cmd string, fns ...func(*ShellOptions)) (proc *exec.Cmd, err error) {
	opts := buildShellOptions(fns)

	path, err := exec.LookPath(cmd)
	if err != nil {
		return
	}

	proc = exec.Command(path, opts.Args...)
	proc.Env = append(os.Environ(), FlattenEnv(opts.Env)...)
	proc.Stdin = io.StdIn()
	proc.Stdout = io.StdOut()
	proc.Stderr = io.StdErr()
	err = proc.Start()
	return
}

func FlattenEnv(data map[string]string) (ret []string) {
	ret = make([]string, 0, len(data))
	for k, v := range data {
		ret = append(ret, fmt.Sprintf("%v=%v", k, v))
	}
	return
}
