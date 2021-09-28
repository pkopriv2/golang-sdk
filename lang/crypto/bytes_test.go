package crypto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBytesEqual_Empty(t *testing.T) {
	assert.True(t, Bytes([]byte{}).Equals([]byte{}))
}

func TestBytesEqual_DiffLength(t *testing.T) {
	assert.False(t, Bytes([]byte{0}).Equals([]byte{}))
}

func TestBytesEqual_EqualLength_DiffVals(t *testing.T) {
	assert.False(t, Bytes([]byte{0}).Equals([]byte{1}))
}

func TestBytesEqual_Equal(t *testing.T) {
	assert.True(t, Bytes([]byte{0, 1}).Equals([]byte{0, 1}))
}
