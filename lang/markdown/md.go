package markdown

import "github.com/russross/blackfriday"

func Render(input string) (ret string) {
	ret = string(blackfriday.Run([]byte(input)))
	return
}
