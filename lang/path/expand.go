package path

import (
	"path"
	"path/filepath"
	"strings"

	homedir "github.com/mitchellh/go-homedir"
)

var (
	Dir = path.Dir
)

func Expand(path string) (ret string, err error) {
	ret = path
	if strings.HasPrefix(path, "~") {
		home, err := homedir.Dir()
		if err != nil {
			return "", err
		}

		ret = filepath.Join(home, path[1:])
	}
	return
}
