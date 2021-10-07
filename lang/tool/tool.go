package tool

import (
	"fmt"

	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/urfave/cli"
)

// We're just going to put a nice functional face on the cli libraries
// and put a thin abstraction that we could conceivably swap if necessary.

// Runs a tool in the given environment and with the provided args.
func Run(e Environment, t Tool, args []string) (err error) {
	if err = t(e).Run(args); err != nil {
		DisplayFailure(e, err)
	}
	return
}

func NewTool(def ToolDef, cmds ...Command) Tool {
	return def.bind(cmds...)
}

func NewGroup(def GroupDef, cmds ...Command) Command {
	return def.bind(cmds...)
}

func NewCommand(def CommandDef) Command {
	return func(env Environment) cli.Command {
		return def.build(env)
	}
}

// A tool returns a runnable app
type Tool func(env Environment) *cli.App

// A group returns a command that is populated with subcommands
type Command func(env Environment) cli.Command

// The standard command handler (basically, we just
// inject an environment)
type Handler func(Environment, *cli.Context) error

// A flag is a simple redirect to a raw flag.
type Flag interface {
	Build() cli.Flag
}

type BoolFlag struct {
	Name  string
	Usage string
}

func (s BoolFlag) Build() cli.Flag {
	return cli.BoolFlag{Name: s.Name, Usage: s.Usage}
}

type IntFlag struct {
	Name    string
	Usage   string
	Default int
}

func (s IntFlag) Build() cli.Flag {
	return cli.IntFlag{Name: s.Name, Usage: s.Usage, Value: s.Default}
}

type UintFlag struct {
	Name    string
	Usage   string
	Default uint
}

func (s UintFlag) Build() cli.Flag {
	return cli.UintFlag{Name: s.Name, Usage: s.Usage, Value: s.Default}
}

type StringFlag struct {
	Name    string
	Usage   string
	Default string
}

func (s StringFlag) Build() cli.Flag {
	return cli.StringFlag{Name: s.Name, Usage: s.Usage, Value: s.Default}
}

type StringsFlag struct {
	Name    string
	Usage   string
	Default []string
}

func (s StringsFlag) Build() cli.Flag {
	return cli.StringSliceFlag{Name: s.Name, Usage: s.Usage, Value: (*cli.StringSlice)(&s.Default)}
}

// Just to make collections of flags a litte easier to deal with
type Flags []Flag

func NewFlags(o ...Flag) Flags {
	return o
}

func (c Flags) Add(o ...Flag) Flags {
	return append(c, o...)
}

// Compiles the list of command of a build
func (c Flags) build() []cli.Flag {
	cmds := make([]cli.Flag, 0, len(c))
	for _, f := range c {
		cmds = append(cmds, f.Build())
	}
	return cmds
}

type ToolDef struct {
	Name    string
	Version string
	Usage   string
	Desc    string
}

func (i ToolDef) bind(all ...Command) Tool {
	return func(env Environment) *cli.App {
		cmds := make([]cli.Command, 0, len(all))
		for _, fn := range all {
			cmds = append(cmds, fn(env))
		}

		return &cli.App{
			Name:                 i.Name,
			Version:              i.Version,
			UsageText:            i.Usage,
			Description:          i.Desc,
			HideHelp:             true,
			EnableBashCompletion: true,
			Commands:             cmds,
		}
	}
}

type CommandDef struct {
	Name  string
	Info  string
	Usage string
	Help  string
	Flags []Flag
	Exec  Handler
}

func (i CommandDef) build(env Environment) cli.Command {
	return cli.Command{
		Name:        i.Name,
		Usage:       i.Info,
		UsageText:   i.Usage,
		Description: i.Help,
		Flags:       Flags(i.Flags).build(),
		Action: func(ctx *cli.Context) (err error) {
			if err = i.Exec(env, ctx); err == nil {
				return
			}

			// FIXME: not the best formatting, but will do.
			if errs.Is(err, errs.ArgError) {
				fmt.Fprintf(env.Terminal.IO.StdErr(), "\n    Usage: %v\n", i.Usage)
			}

			return err
		},
	}
}

type GroupDef struct {
	Name  string
	Info  string
	Usage string
}

func (i GroupDef) bind(all ...Command) Command {
	return func(env Environment) cli.Command {
		cmds := make([]cli.Command, 0, len(all))
		for _, cmd := range all {
			cmds = append(cmds, cmd(env))
		}
		return cli.Command{
			Name:        i.Name,
			Usage:       i.Info,
			UsageText:   i.Info,
			Subcommands: cmds,
		}
	}
}
