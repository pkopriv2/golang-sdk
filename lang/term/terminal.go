package term

import (
	"os/user"

	homedir "github.com/mitchellh/go-homedir"
)

var SystemTerminal Terminal = func() Terminal {
	home, err := homedir.Dir()
	if err != nil {
		home = "unknown"
	}

	login, err := user.Current()
	if err != nil {
		login = &user.User{Username: "unknown"}
	}

	return Terminal{home, login.Username, SystemShell, SystemIO}
}()

type Terminal struct {
	Home  string
	User  string
	Shell Shell
	IO    IO
}

func (t Terminal) Prompt(prompt string, dest *[]byte, fns ...LineOption) error {
	return ReadLine(t.IO, prompt, SetPointer(dest), fns...)
}

func (t Terminal) PromptPassword(prompt string, dest *[]byte) error {
	return PromptPassword(t.IO, prompt, dest)
}
