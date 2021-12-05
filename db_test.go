package simplekv

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/stretchr/testify/assert"
)

// delete the db data file folder before each test.
func deleteFolder() {
	err := os.RemoveAll(config.DBDir)
	if err != nil {
		panic(err)
	}
}

func TestBasicIO(t *testing.T) {
	deleteFolder()
	MustLoad()

	err := Write("hello", "world")
	assert.NoError(t, err)

	v, err := Get("hello")
	assert.NoError(t, err)
	assert.Equal(t, "world", v)

	err = Write("hello", "world2")
	assert.NoError(t, err)

	v, err = Get("hello")
	assert.NoError(t, err)
	assert.Equal(t, "world2", v)

	err = Delete("hello")
	assert.NoError(t, err)

	_, err = Get("hello")
	assert.ErrorIs(t, err, config.ErrRecordNotFound)
}

func TestLoadTwice(t *testing.T) {
	deleteFolder()

	var writeNoErr = func(k, v string) {
		err := Write(k, v)
		assert.NoError(t, err)
	}

	var expireNoErr = func(k string, ms int) {
		err := Expire(k, ms)
		assert.NoError(t, err)
	}

	MustLoad()

	writeNoErr("hello", "world")
	expireNoErr("hello", 1)
	writeNoErr("hello", "world2")

	time.Sleep(time.Second)

	v, err := Get("hello")
	assert.ErrorIs(t, err, config.ErrRecordExpired)
	assert.Equal(t, "", v)

	MustLoad()

	v, err = Get("hello")
	assert.ErrorIs(t, err, config.ErrRecordNotFound)
	assert.Equal(t, "", v)
}

func TestExpire(t *testing.T) {
	deleteFolder()
	MustLoad()

	var writeNoErr = func(k, v string) {
		err := Write(k, v)
		assert.NoError(t, err)
	}

	var expireNoErr = func(k string, ms int) {
		err := Expire(k, ms)
		assert.NoError(t, err)
	}

	const setTTL = 2

	writeNoErr("hello", "world")

	ttl, err := TTL("hello")
	assert.ErrorIs(t, err, config.ErrNoRelatedExpire)
	assert.Equal(t, uint32(0), ttl)

	expireNoErr("hello", setTTL)

	ttl, err = TTL("hello")
	assert.NoError(t, err)
	assert.Equal(t, true, ttl <= setTTL)

	ttl1 := ttl

	ttl, err = TTL("no such key")
	assert.ErrorIs(t, err, config.ErrRecordNotFound)
	assert.Equal(t, uint32(0), ttl)

	// update the value should not affect ttl
	writeNoErr("hello", "world2")

	ttl2, err := TTL("hello") // warning small chance to fail here
	assert.NoError(t, err)
	assert.Equal(t, true, ttl1 == ttl2)

	val, err := Get("hello")
	assert.NoError(t, err)
	assert.NotNil(t, val)

	MustLoad()

	// after reload, expire time should be the same as the one before load

	val, err = Get("hello")
	assert.NoError(t, err)
	assert.NotNil(t, val)

	ttl, err = TTL("hello")
	assert.NoError(t, err)
	assert.Equal(t, true, ttl <= setTTL)

	time.Sleep(setTTL * time.Second)

	// cause the record to expire

	val, err = Get("hello")
	assert.ErrorIs(t, err, config.ErrRecordExpired)
	assert.Equal(t, "", val)

	val, err = Get("hello")
	assert.ErrorIs(t, err, config.ErrRecordNotFound)
	assert.Equal(t, "", val)

}

func TestMultipleFiles(t *testing.T) {
	config.BlockSize = 64
	deleteFolder()
	MustLoad()

	getKey := func(i int) string {
		return fmt.Sprintf("simpledb:testkey:%d", i)
	}
	getVal := func(i int) string {
		return strconv.FormatInt(int64(i), 10)
	}

	const keyCnt = 10

	for i := 0; i < keyCnt; i++ {
		Write(getKey(i), getVal(i))
	}

	var readCheck = func() {
		for i := 0; i < keyCnt; i++ {
			v, err := Get(getKey(i))
			assert.NoError(t, err)
			assert.Equal(t, getVal(i), v)
		}
	}

	readCheck()
	MustLoad()
	readCheck()
}
