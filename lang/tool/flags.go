package tool

import (
	"github.com/pkopriv2/golang-sdk/libs/page"
	"github.com/urfave/cli"
)

var (
	LFlag = BoolFlag{
		Name:  "l",
		Usage: "Detailed listing",
	}

	VFlag = BoolFlag{
		Name:  "v",
		Usage: "Verbose output",
	}

	PageBegFlag = UintFlag{
		Name:    "offset",
		Usage:   "Starting offset of results",
		Default: 0,
	}

	PageMaxFlag = UintFlag{
		Name:    "n",
		Usage:   "Maximum number of results",
		Default: 256,
	}

	PageDelFlag = BoolFlag{
		Name:  "d",
		Usage: "Show deleted files",
	}

	PageFlags = NewFlags(PageBegFlag, PageMaxFlag)
)

func ParsePageOpts(cli *cli.Context) (ret []page.PageOption) {
	if offset := cli.Int(PageBegFlag.Name); offset > 0 {
		ret = append(ret, page.Offset(uint64(offset)))
	}
	if limit := cli.Int(PageMaxFlag.Name); limit > 0 {
		ret = append(ret, page.Limit(uint64(limit)))
	}
	return
}
