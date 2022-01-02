package util

import (
	"io"
	"os"
	"strings"
)

func MustGetFileSize(fp *os.File) int64 {
	// read through the file to get its size
	buf := [2048]byte{}
	sz := int64(0)
	for {
		cnt, err := fp.Read(buf[:])
		sz += int64(cnt)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
	}
	return sz
}

func ExtractFileName(path string) string {
	tmp := strings.Split(path, "/")
	return tmp[len(tmp)-1]
}

func MustOpenFileAppend(path string) *os.File {
	fp, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	return fp
}

func MustOpenFileRndRW(path string) *os.File {
	fp, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	return fp
}
