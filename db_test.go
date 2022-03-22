package simplekv

import (
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/dbfile"
	"github.com/stretchr/testify/assert"
)

func TestBasicIO(t *testing.T) {
	testBootServerStandalone()
	defer ShutdownServerGracefully(true)

	err := Write("hello", "world", 0)
	assert.NoError(t, err)

	v, err := Get("hello")
	assert.NoError(t, err)
	assert.Equal(t, "world", v)

	err = Write("hello", "world2", 0)
	assert.NoError(t, err)

	v, err = Get("hello")
	assert.NoError(t, err)
	assert.Equal(t, "world2", v)

	err = Delete("hello", 0)
	assert.NoError(t, err)

	_, err = Get("hello")
	assert.ErrorIs(t, err, config.ErrRecordNotFound)

}

func TestLoadTwice(t *testing.T) {

	var writeNoErr = func(k, v string) {
		err := Write(k, v, 0)
		assert.NoError(t, err)
	}

	var expireNoErr = func(k string, ms int) {
		err := Expire(k, ms, 0)
		assert.NoError(t, err)
	}

	testBootServerStandalone()

	writeNoErr("hello", "world")
	expireNoErr("hello", 1)
	writeNoErr("hello", "world2")

	time.Sleep(time.Second)

	v, err := Get("hello")
	assert.ErrorIs(t, err, config.ErrRecordExpired)
	assert.Equal(t, "", v)

	// server restart and wait until OK
	testRestartServerAndWaitUntilOK()

	v, err = Get("hello")
	assert.ErrorIs(t, err, config.ErrRecordNotFound)
	assert.Equal(t, "", v)

	ShutdownServerGracefully(true)
}

func TestExpire(t *testing.T) {

	var writeNoErr = func(k, v string) {
		err := Write(k, v, 0)
		assert.NoError(t, err)
	}

	var expireNoErr = func(k string, ms int) {
		err := Expire(k, ms, 0)
		assert.NoError(t, err)
	}

	const setTTL = 2

	testBootServerStandalone()

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

	testRestartServerAndWaitUntilOK()

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

	ShutdownServerGracefully(true)

}

func TestMultipleFiles(t *testing.T) {
	config.DataBlockSize = 64

	testBootServerStandalone()

	getKey := func(i int) string {
		return fmt.Sprintf("simpledb:testkey:%d", i)
	}
	getVal := func(i int) string {
		return strconv.FormatInt(int64(i), 10)
	}

	const keyCnt = 10

	for i := 0; i < keyCnt; i++ {
		Write(getKey(i), getVal(i), 0)
	}

	var readCheck = func() {
		for i := 0; i < keyCnt; i++ {
			v, err := Get(getKey(i))
			assert.NoError(t, err)
			assert.Equal(t, getVal(i), v)
		}
	}

	readCheck()
	testRestartServerAndWaitUntilOK()
	readCheck()

	ShutdownServerGracefully(true)
}

// This test contain some problems and will not pass now.
func TestReadEntries(t *testing.T) {
	testBootServerStandalone()

	getKey := func(i int) string {
		return fmt.Sprintf("simpledb:testkey:%d", i)
	}
	getVal := func(i int) string {
		return strconv.FormatInt(int64(i), 10)
	}

	const keyCnt = 10

	for i := 0; i < keyCnt; i++ {
		Write(getKey(i), getVal(i), 0)
	}

	timerRecorder := time.Now()
	var timeElapsed = func() {
		t.Log("time elapsed = ", time.Since(timerRecorder).Microseconds(), "us")
		timerRecorder = time.Now()
	}

	// case 1: Normal read from start.
	entriesRaw, err := dataInstance.readEntries(0, 10)
	timeElapsed()
	assert.NoError(t, err)
	assert.Len(t, entriesRaw, 10)
	for i, eachEntryRaw := range entriesRaw {
		e := dbfile.Decode(eachEntryRaw)
		assert.Equal(t, getVal(i), string(e.Value))
	}

	// case 2: Read an infinite count of entries from start.
	entriesRaw, err = dataInstance.readEntries(0, 1000000)
	timeElapsed()
	assert.ErrorIs(t, err, io.EOF)
	assert.Len(t, entriesRaw, 10)
	for i, eachEntryRaw := range entriesRaw {
		e := dbfile.Decode(eachEntryRaw)
		assert.Equal(t, getVal(i), string(e.Value))
	}

	// case 3: Random read will cause broken data.
	_, err = dataInstance.readEntries(232, 5)
	timeElapsed()
	assert.ErrorIs(t, err, config.ErrBrokenData)

	// case 4: Read not exist entries.
	entriesRaw, err = dataInstance.readEntries(99999, 10)
	timeElapsed()
	assert.ErrorIs(t, err, config.ErrFileNotFound)
	assert.Len(t, entriesRaw, 0)

	// case 5: Normal read from middle with a limited count assigned.
	entriesRaw, err = dataInstance.readEntries(36, 1) // the single size of an entry in this test is 36.
	timeElapsed()
	assert.NoError(t, err)
	assert.Len(t, entriesRaw, 1)

	// case 6: Read 0 entry from a random position.
	entriesRaw, err = dataInstance.readEntries(56, 0)
	timeElapsed()
	assert.NoError(t, err)
	assert.Len(t, entriesRaw, 0)

	ShutdownServerGracefully(true)
}
