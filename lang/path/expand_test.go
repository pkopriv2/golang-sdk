package path

import (
	"fmt"
	"testing"
)

func TestExpand_Empty(t *testing.T) {
	fmt.Println(Expand("~/hello/a"))
}
