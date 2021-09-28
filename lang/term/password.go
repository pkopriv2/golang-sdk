package term

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh/terminal"
)

func PromptPassword(io IO, prompt string, dest *[]byte) error {
	if prompt != "" {
		if _, err := fmt.Fprint(io.StdOut(), prompt+": "); err != nil {
			return err
		}
	}

	f, ok := io.StdIn().(*os.File)
	if !ok {
		return errors.Errorf("Cannot obtain file descriptor on input IO")
	}

	raw, err := terminal.ReadPassword(int(f.Fd()))
	if err != nil {
		return err
	}

	if prompt != "" {
		if _, err := fmt.Fprint(io.StdOut(), "\n"); err != nil {
			return err
		}
	}

	*dest = raw
	return nil
}
