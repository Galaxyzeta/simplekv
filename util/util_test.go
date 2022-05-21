package util

import (
	"os"
	"sync"
	"testing"
	"time"
)

func TestGetFileSize(t *testing.T) {
	fp, err := os.Open("../tmp/test_file.txt")
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	sz := MustGetFileSize(fp)
	t.Log(sz)
}

func TestCondBlocker(t *testing.T) {
	// test broadcast before wait
	state := 0
	condIsStateEq1 := NewConditionBlocker(func() bool {
		return state == 1
	})
	go func() {
		state = 1
		condIsStateEq1.Broadcast()
	}()
	time.Sleep(time.Second)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		condIsStateEq1.LoopWaitUntilTrue()
		wg.Done()
	}()
	wg.Wait()
	t.Log("phase 1 ok")

	// test wait before broadcast
	state = 0
	wg.Add(1)
	go func() {
		condIsStateEq1.LoopWaitUntilTrue()
		wg.Done()
	}()
	time.Sleep(time.Second)
	go func() {
		state = 1
		condIsStateEq1.Broadcast()
	}()
	wg.Wait()
	t.Log("phase 2 ok")
}

func TestLogger(t *testing.T) {
	logger := NewLogger("[Test]", os.Stdout, true)
	logger.Infof("hello")
}

type replicationRecord struct {
	offset int
}

func (i replicationRecord) Get() int {
	return i.offset
}

func TestHashQueue(t *testing.T) {
	hq := NewIntHashQueue[replicationRecord]()
	offsets := []int{
		1, 2, 3, 4, 5,
	}
	for _, eachoffset := range offsets {
		hq.Enqueue(&replicationRecord{
			offset: eachoffset,
		})
	}
	hq.TryComplete(func(v *replicationRecord) bool {
		return v.offset < 3
	})
	t.Log(hq.Head().Get())
}
