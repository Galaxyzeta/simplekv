package dbfile

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestByteUintConvert(t *testing.T) {
	var a uint32 = 252117761
	t.Log(uint8(a >> 24))
	t.Log(uint8(a >> 16))
	t.Log(uint8(a >> 8))
	t.Log(uint8(a))
}

func TestEntryEncode(t *testing.T) {
	// TODO rewrite
	// encode
	e := NewEntryWithAll("hello", "world", 0, 0, ExtraEnum_Delete)

	assert.Equal(t, ExtraEnum_Delete, e.Extra)

	ret := e.Encode()

	// decode
	e = Decode(ret)
	assert.Equal(t, uint8(5), e.KLen)
	assert.Equal(t, uint32(5), e.VLen)
	assert.Equal(t, uint32(0), e.ExpireAt)
	assert.Equal(t, "hello", string(e.Key))
	assert.Equal(t, "world", string(e.Value))
}
