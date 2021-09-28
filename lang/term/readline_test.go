package term

import (
	"fmt"
	"testing"
)

func TestReadline(t *testing.T) {

	var str string
	err := ReadLine(SystemIO, "test", SetString(&str), WithDefault("Default"))
	fmt.Println(str)
	fmt.Println(err)
}
