package util

import (
	"os"
	"testing"
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
