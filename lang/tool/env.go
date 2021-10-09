package tool

import (
	"os"

	"github.com/pkg/errors"
	"github.com/pkopriv2/golang-sdk/lang/config"
	"github.com/pkopriv2/golang-sdk/lang/context"
	"github.com/pkopriv2/golang-sdk/lang/path"
	"github.com/pkopriv2/golang-sdk/lang/term"
)

type Environment struct {
	Context  context.Context
	Config   config.Config
	Terminal term.Terminal
}

func NewEnvironment(configFile string) (ret Environment, err error) {
	path, err := path.Expand(configFile)
	if err != nil {
		err = errors.Wrapf(err, "Unable to expand path [%v]", configFile)
		return
	}

	conf := config.NewConfig()
	if _, err := os.Stat(path); err == nil {
		conf, err = config.ParseFromFile(configFile)
		if err != nil {
			err = errors.Wrapf(err, "Unable to parse config file [%v]", configFile)
			return ret, err
		}
	}

	var lvl string
	if err = conf.GetOrDefault("log.level", config.String, &lvl, context.Off.String()); err != nil {
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
