package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLRUCache(t *testing.T) {
	// phase 1
	var c Cacher = NewLRUCache(2)
	c.Store("hello", "world")
	c.Store("foo", "bar")
	val, ok := c.Load("hello")
	assert.Equal(t, true, ok)
	assert.Equal(t, "world", val)
	val, ok = c.Load("foo")
	assert.Equal(t, true, ok)
	assert.Equal(t, "bar", val)

	// phase 2
	c.Store("hello", "world2")
	val, ok = c.Load("hello")
	assert.Equal(t, true, ok)
	assert.Equal(t, "world2", val)
	val, ok = c.Load("foo") // cause hello to become the last one
	assert.Equal(t, true, ok)
	assert.Equal(t, "bar", val)

	// phase 3
	c.Store("gin", "gonic") // cause hello to get evicted
	val, ok = c.Load("gin")
	assert.Equal(t, true, ok)
	assert.Equal(t, "gonic", val)
	val, ok = c.Load("hello")
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, val)

	// phase 4
	val, ok = c.Load("what")
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, val)

	// phase 5
	c.Store("galaxy", "zeta") // cause foo to get evicted
	val, ok = c.Load("foo")
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, val)

}
