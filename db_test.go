package simplekv

import (
	"fmt"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/dbfile"
	"github.com/galaxyzeta/simplekv/util"
	"github.com/stretchr/testify/assert"
)

func testTimeEstimate(t *testing.T, fn func()) {
	timeStart := time.Now()
	fn()
	t.Logf("TIME = %d ms", time.Since(timeStart).Milliseconds())
}

func testSetNoErr(t *testing.T, k, v string, requiredAck int) {
	assert.NoError(t, Write(k, v, requiredAck))
}

func testDeleteNoErr(t *testing.T, k string, requiredAck int) {
	assert.NoError(t, Delete(k, requiredAck))
}

func testExpireNoErr(t *testing.T, k string, ttl int, requiredAck int) {
	assert.NoError(t, Expire(k, ttl, requiredAck))
}

func testExpireExpectError(t *testing.T, k string, ttl int, requiredAck int, expectedError error) {
	err := Expire(k, ttl, requiredAck)
	assert.Equal(t, expectedError, err)
}

func testGetNoErrAndExpect(t *testing.T, k, expect string) {
	v, err := Get(k)
	if err != nil {
		assert.NoError(t, err)
		assert.Equal(t, expect, v)
	}
}

func testGetExpectError(t *testing.T, k string, expectedError error) {
	_, err := Get(k)
	assert.Equal(t, expectedError, err)
}

func TestBasicIO(t *testing.T) {
	testBootServerStandalone()
	defer ShutdownServerGracefully(true)
	requiredAck := 0

	testGetExpectError(t, "a", config.ErrRecordNotFound)
	testSetNoErr(t, "a", "123", requiredAck)
	testGetNoErrAndExpect(t, "a", "123")
	testSetNoErr(t, "b", "hello world", requiredAck)
	testGetNoErrAndExpect(t, "b", "hello world")
	testDeleteNoErr(t, "a", requiredAck)
	testDeleteNoErr(t, "a", requiredAck)
	testExpireNoErr(t, "b", 2, requiredAck)
	testGetNoErrAndExpect(t, "b", "2")
	time.Sleep(time.Second * 2)
	testGetExpectError(t, "b", config.ErrRecordNotFound)
}

func TestBasicIOWithRestart(t *testing.T) {
	testBootServerStandalone()
	defer ShutdownServerGracefully(true)
	requiredAck := 0

	testSetNoErr(t, "asd", "qwe", requiredAck)
	testSetNoErr(t, "asd2", "qwe2", requiredAck)
	testExpireNoErr(t, "asd", 1, requiredAck)
	testRestartServerAndWaitUntilOK()
	time.Sleep(time.Second)
	testGetExpectError(t, "asd", config.ErrRecordNotFound)
	testGetNoErrAndExpect(t, "asd2", "qwe2")
}

func TestCacheHitRate(t *testing.T) {
	const iterationCount = 10
	testBootServerStandalone(func() {
		config.DataLruCapacity = 5
	})
	defer ShutdownServerGracefully(true)
	requiredAck := 0

	getKey := func(i int) string {
		return fmt.Sprintf("hello-%d", i)
	}
	getVal := func(i int) string {
		return strconv.FormatInt(int64(i), 10)
	}
	for i := range util.Iter(iterationCount) {
		testSetNoErr(t, getKey(i), getVal(i), requiredAck)
	}
	for i := range util.Iter(iterationCount) {
		testGetNoErrAndExpect(t, getKey(i), getVal(i))
	}
	for i := iterationCount; i < iterationCount*2; i++ {
		testGetExpectError(t, getKey(i), config.ErrRecordNotFound)
	}
	t.Log(ctrlInstance.stats.cacheStats.toString())
}

func TestConcurrentIO(t *testing.T) {
	const iterationCount = 10
	testBootServerStandalone(func() {
		config.DataLruCapacity = 1000
	})
	defer ShutdownServerGracefully(true)
	getKey := func(n int) string {
		return fmt.Sprintf("test-%d", n)
	}
	getValue := func(n int) string {
		return fmt.Sprintf("%d", n)
	}
	const iteration = 1000
	const requiredAck = 0
	wg := sync.WaitGroup{}
	for i := range util.Iter(iteration) {
		wg.Add(1)
		go func(val int) {
			if e := Write(getKey(val), getValue(val), requiredAck); e != nil {
				t.Fail()
				t.Log(e)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	for i := range util.Iter(iteration) {
		wg.Add(1)
		go func(val int) {
			if v, e := Get(getKey(val)); e != nil || v != getValue(val) {
				t.Fail()
				t.Log(e)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestBasicOpTimecost(t *testing.T) {
	testBootServerStandalone()
	iteration := 10000
	arr := make([][]string, iteration)
	it := util.Iter(iteration)
	for i := range it {
		arr[i] = ([]string{
			fmt.Sprintf("hello-%d", i),
			strconv.FormatInt(int64(i), 10),
		})
	}
	requiredAck := 1
	testTimeEstimate(t, func() {
		for i := range it {
			Write(arr[i][0], arr[i][1], requiredAck)
			// Get(arr[i][0])
		}
	})
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
	assert.ErrorIs(t, err, config.ErrRecordNotFound)
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
	assert.ErrorIs(t, err, config.ErrRecordNotFound)
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
