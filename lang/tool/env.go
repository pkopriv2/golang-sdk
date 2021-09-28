package tool

import (
	"os"

	"github.com/pkopriv2/golang-sdk/lang/config"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/path"
	"github.com/pkopriv2/golang-sdk/lang/term"
	"github.com/pkg/errors"
)

var DefaultConfigFile = "~/.stash/config.yaml"

func init() {
	if path := os.Getenv("STASH_CONFIG_FILE"); path != "" {
		DefaultConfigFile = path
	}
}

type Environment struct {
	Context  context.Context
	Config   config.Config
	Terminal term.Terminal
}

func NewDefaultEnvironment() (ret Environment, err error) {
	path, err := path.Expand(DefaultConfigFile)
	if err != nil {
		err = errors.Wrapf(err, "Unable to expand path [%v]", DefaultConfigFile)
		return
	}

	conf := config.NewConfig()
	if _, err := os.Stat(path); err == nil {
		conf, err = config.ParseFromFile(DefaultConfigFile)
		if err != nil {
			err = errors.Wrapf(err, "Unable to parse config file [%v]", DefaultConfigFile)
			return ret, err
		}
	}

	var lvl string
	if err = conf.GetOrDefault("stash.log.level", config.String, &lvl, context.Off.String()); err != nil {
		return
	}

	log, err := context.ParseLogLevel(lvl)
	if err != nil {
		return
	}

	ret = Environment{
		Context:  context.NewContext(os.Stdout, log),
		Config:   conf,
		Terminal: term.SystemTerminal,
	}
	return
}
