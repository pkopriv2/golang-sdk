package crypto

import (
	"crypto/rand"
	"io"
	"math/big"
	"strings"

	"github.com/sethvargo/go-diceware/diceware"
)

// Returns the number of diceware words to include in a diceware based password.
func numDiceWords(s Strength) int {
	switch s {
	default:
		return 5
	case Weak, Minimal:
		return 6
	case Moderate:
		return 6
	case Strong:
		return 7
	case Maximum:
		return 8
	}
}

// Generates an array of diceware words.  This may be used as the basis
// for symmetric encryption key schemes or password based authentication.
func GenDiceWords(rand io.Reader, strength Strength) (ret []string, err error) {
	ret, err = diceware.Generate(numDiceWords(strength))
	return
}

// Generates an array of diceware words.  This may be used as the basis
// for symmetric encryption key schemes or password based authentication.
func GenDicePass(rand io.Reader, strength Strength) (ret string, err error) {
	words, err := GenDiceWords(rand, strength)
	if err != nil {
		return
	}

	ret = strings.Join(words, "-")
	return
}

type PassOption func(*PassOptions)

type PassOptions struct {
	Strength  Strength
	UpperCase bool
	Numbers   bool
	Symbols   bool
}

func buildPassOptions(ops ...PassOption) (ret PassOptions) {
	ret = PassOptions{Strength: Moderate, UpperCase: false}
	for _, fn := range ops {
		fn(&ret)
	}
	return
}

func WithPassStrength(s Strength) PassOption {
	return func(p *PassOptions) {
		p.Strength = s
	}
}

func WithNumbers() PassOption {
	return func(p *PassOptions) {
		p.Numbers = true

	}
}

func WithSymbols() PassOption {
	return func(p *PassOptions) {
		p.Symbols = true
	}
}

func PassLength(s Strength) int {
	switch s {
	default:
		return 10
	case Minimal:
		return 8
	case Moderate:
		return 10
	case Strong:
		return 24
	case Maximum:
		return 32
	}
}

func GenPass(r io.Reader, ops ...PassOption) (ret string, err error) {
	opts := buildPassOptions(ops...)

	chars := lowerCase
	if opts.UpperCase {
		chars = append(chars, upperCase...)
	}
	if opts.Numbers {
		chars = append(chars, numbers...)
	}
	if opts.Symbols {
		chars = append(chars, symbols...)
	}

	num := PassLength(opts.Strength)
	out := make([]rune, 0, num)

	max := big.NewInt(int64(len(chars)))
	for i := 0; i < num; i++ {
		n, er := rand.Int(r, max)
		if er != nil {
			err = er
			return
		}

		out = append(out, chars[n.Int64()])
	}
	ret = string(out)
	return
}

var (
	lowerCase = []rune{
		'a',
		'b',
		'c',
		'd',
		'e',
		'f',
		'g',
		'h',
		'i',
		'j',
		'k',
		'l',
		'm',
		'n',
		'o',
		'p',
		'q',
		'r',
		's',
		't',
		'u',
		'v',
		'w',
		'x',
		'y',
		'z',
	}

	upperCase = []rune{
		'A',
		'B',
		'C',
		'D',
		'E',
		'F',
		'G',
		'H',
		'I',
		'J',
		'K',
		'L',
		'M',
		'N',
		'O',
		'P',
		'Q',
		'R',
		'S',
		'T',
		'U',
		'V',
		'W',
		'X',
		'Y',
		'Z',
	}

	numbers = []rune{
		'0',
		'1',
		'2',
		'3',
		'4',
		'5',
		'6',
		'7',
		'8',
		'9',
	}

	symbols = []rune{
		'!',
		'@',
		'#',
		'$',
		'%',
		'^',
		'&',
		'*',
		'(',
		')',
		'-',
		'_',
		'+',
		'=',
	}
)
