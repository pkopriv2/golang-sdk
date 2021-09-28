package secret

import (
	"fmt"
	"io"
	"math/big"

	"github.com/pkopriv2/golang-sdk/lang/crypto"
	"github.com/pkopriv2/golang-sdk/lang/enc"
	"github.com/pkopriv2/golang-sdk/lang/errs"
	"github.com/pkg/errors"
)

var (
	Lines Type = "lines/0.1"
)

type LineSecret struct {
	line    line
	entropy int
}

func generateLineSecret(rand io.Reader, entropy int) (ret Secret, err error) {
	line, err := generateLine(rand, entropy)
	if err != nil {
		return
	}
	return &LineSecret{line, entropy}, nil
}

func (s *LineSecret) Shard(rand io.Reader) (ret Shard, err error) {
	pt, err := generatePoint(rand, s.line, s.entropy)
	if err != nil {
		return
	}
	return &LineShard{pt, s.entropy}, nil
}

func (s *LineSecret) Hash(h crypto.Hash) (crypto.Bytes, error) {
	return s.line.Hash(h)
}

func (s *LineSecret) Destroy() {
	s.line.Destroy()
}

func (s *LineSecret) String() string {
	return fmt.Sprintf("Line: %v", s.line)
}

type LineShard struct {
	Pt      Point
	Entropy int
}

func (s *LineShard) Type() Type {
	return Lines
}

func (s *LineShard) Derive(raw Shard) (ret Secret, err error) {
	sh, ok := raw.(*LineShard)
	if !ok {
		err = errors.Wrap(errs.ArgError, "Incompatible shards. Wrong type.")
		return
	}

	if s.Entropy != sh.Entropy {
		err = errors.Wrap(errs.ArgError, "Incompatible shards. Wrong type.")
		return
	}

	line, err := s.Pt.Derive(sh.Pt)
	if err != nil {
		return
	}

	ret = &LineSecret{line, s.Entropy}
	return
}

func (l *LineShard) MarshalJSON() (ret []byte, err error) {
	err = enc.Json.EncodeBinary(struct {
		E int      `json:"entropy"`
		X *big.Int `json:"x,string"`
		Y *big.Int `json:"y,string"`
	}{
		l.Entropy,
		l.Pt.X,
		l.Pt.Y,
	}, &ret)
	return
}

func (l *LineShard) UnmarshalJSON(data []byte) (err error) {
	l.Pt = Point{X: &big.Int{}, Y: &big.Int{}}
	err = enc.Json.DecodeBinary(data, &struct {
		E *int     `json:"entropy"`
		X *big.Int `json:"x,string"`
		Y *big.Int `json:"y,string"`
	}{
		&l.Entropy,
		l.Pt.X,
		l.Pt.Y,
	})
	return
}

func (s *LineShard) Destroy() {
	s.Pt.Destroy()
}

// Generates a random line.  The domain is used to determine the number of bytes to use when generating
// the properties of the curve.
func generateLine(rand io.Reader, domain int) (line, error) {
	slope, err := generateBigInt(rand, domain)
	if err != nil {
		return line{}, errors.WithStack(err)
	}

	intercept, err := generateBigInt(rand, domain)
	if err != nil {
		return line{}, errors.WithStack(err)
	}

	return line{slope, intercept}, nil
}

// Generates a random Point on the line.  The domain is used to bound the size of the resulting Point.
func generatePoint(rand io.Reader, line line, domain int) (Point, error) {
	x, err := generateBigInt(rand, domain)
	if err != nil {
		return Point{}, errors.Wrapf(err, "Unable to generate Point on line [%v] for domain [%v]", line, domain)
	}
	return line.Point(x), nil
}

// TODO: Is generating a random byte array consistent with generating a random integer?
//
// Generates a random integer using the size to determine the number of bytes to use when generating the
// random value.
func generateBigInt(rand io.Reader, size int) (*big.Int, error) {
	buf, err := crypto.GenNonce(rand, size)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to generate random *big.Int of size [%v]", size)
	}

	return new(big.Int).SetBytes(buf), nil
}

// A simple y=mx+b line.  Considered a parametric form, but when moving to a
// canonical form, it reduces to this anyway.
type line struct {
	Slope     *big.Int
	Intercept *big.Int
}

func (l line) Destroy() {
	sBytes := l.Slope.Bytes()
	iBytes := l.Intercept.Bytes()

	crypto.NewBytes(sBytes).Destroy()
	crypto.NewBytes(iBytes).Destroy()

	l.Slope.SetBytes(sBytes)
	l.Slope.SetBytes(iBytes)
}

func (l line) Height(x *big.Int) *big.Int {
	ret := big.NewInt(0)
	ret.Mul(x, l.Slope).Add(ret, l.Intercept)
	return ret
}

func (l line) Contains(o Point) bool {
	return l.Point(o.X).Equals(o)
}

func (l line) Point(x *big.Int) Point {
	return Point{x, l.Height(x)}
}

func (l line) Equals(o line) bool {
	return (l.Slope.Cmp(o.Slope) == 0) && (l.Intercept.Cmp(o.Intercept) == 0)
}

func (l line) String() string {
	return fmt.Sprintf("Line(m=%v,y-intercept=%v)", l.Slope, l.Intercept)
}

// consistent byte representation of line
func (l line) Hash(hash crypto.Hash) ([]byte, error) {
	return hash.Hash(append(l.Slope.Bytes(), l.Intercept.Bytes()...))
}

// A vector representation of a Point in 2-dimensional space
type Point struct {
	X *big.Int
	Y *big.Int
}

func (p Point) Destroy() {
	xBytes := p.X.Bytes()
	yBytes := p.Y.Bytes()

	crypto.NewBytes(xBytes).Destroy()
	crypto.NewBytes(yBytes).Destroy()

	p.X.SetBytes(xBytes)
	p.Y.SetBytes(yBytes)
}

func (p Point) Derive(o Point) (line, error) {
	if p.X.Cmp(o.X) == 0 {
		return line{}, errors.Errorf("Cannot derive a line from the same Points [%v,%v]", o, p)
	}
	slope := deriveSlope(p, o)
	return line{slope, deriveIntercept(p, slope)}, nil
}

func (p Point) Equals(o Point) bool {
	return (p.X.Cmp(o.X) == 0) && (p.Y.Cmp(o.Y) == 0)
}

func (p Point) String() string {
	return fmt.Sprintf("Point(%v,%v)", p.X, p.Y)
}

func deriveSlope(p1, p2 Point) *big.Int {
	delX := big.NewInt(0)
	delX.Sub(p2.X, p1.X)

	delY := big.NewInt(0)
	delY.Sub(p2.Y, p1.Y)

	return delY.Div(delY, delX)
}

func deriveIntercept(p Point, slope *big.Int) *big.Int {
	delY := big.NewInt(0)
	delY.Mul(p.X, slope)

	ret := big.NewInt(0)
	return ret.Sub(p.Y, delY)
}
