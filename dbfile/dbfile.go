package dbfile

import (
	"io"
	"os"
	"sync"

	"github.com/galaxyzeta/simplekv/util"
)

type ExtraEnum uint8

const (
	ExtraEnum_Unknown ExtraEnum = iota
	ExtraEnum_Delete
)

type Entry struct {
	KLen   uint8
	VLen   uint32
	Expire uint32
	Extra  ExtraEnum
	Key    []byte
	Value  []byte
}

type File struct {
	name   string
	fp     *os.File
	offset int64
	mu     sync.RWMutex
}

type FileWalkFn func(e Entry)

func (file *File) Offset() int64 {
	file.mu.RLock()
	defer file.mu.RUnlock()
	return file.offset
}

func (file *File) File() *os.File {
	file.mu.RLock()
	defer file.mu.RUnlock()
	return file.fp
}

func (file *File) Name() string {
	return file.name
}

func (file *File) Write(stream []byte) (err error) {
	fp := file.File()
	if _, err = fp.Write(stream); err != nil {
		return
	}
	file.offset += int64(len(stream))
	return
}

func (file *File) Read(offset int64) (e Entry, err error) {
	return readSingleEntryAt(file.File(), offset)
}

func (file *File) Walk(fn FileWalkFn) {
	file.mu.Lock()
	fp := file.fp
	file.mu.Unlock()
	fp.Seek(0, 0)
	for {
		e, err := readSingleEntry(fp)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		fn(e)
	}
}

func readSingleEntryAt(fp *os.File, offset int64) (e Entry, err error) {
	fp.Seek(offset, 0)
	return readSingleEntry(fp)
}

func readSingleEntry(fp *os.File) (e Entry, err error) {
	buffer := make([]byte, 10)
	if _, err = fp.Read(buffer); err != nil {
		return
	}
	e = Entry{
		KLen:   uint8(buffer[0]),
		VLen:   util.Bytes2Uint32(buffer[1:5]),
		Expire: util.Bytes2Uint32(buffer[5:9]),
		Extra:  ExtraEnum(buffer[9]),
	}
	buffer = make([]byte, int(e.KLen)+int(e.VLen))
	fp.Read(buffer)
	e.Key = buffer[0:e.KLen]
	e.Value = buffer[e.KLen:]
	return
}

func NewEntry(key string, value string) (e Entry) {
	// TODO: validate key and value length
	e.KLen = uint8(len(key))
	e.VLen = uint32(len(value))
	e.Key = []byte(key)
	e.Value = []byte(value)
	return
}

func NewEntryWithExpire(key string, value string, expire uint32) (e Entry) {
	e = NewEntry(key, value)
	e.Expire = expire
	return
}

func (e Entry) Encode() (ret []byte) {
	klenInt := int(e.KLen)
	vlenInt := int(e.VLen)
	totalLen := e.Size()
	ret = make([]byte, totalLen)

	ret[0] = e.KLen
	copy(ret[1:5], util.Uint32ToBytes(e.VLen))
	copy(ret[5:9], util.Uint32ToBytes(e.Expire))
	ret[9] = uint8(e.Extra)

	pos := 10 + klenInt
	copy(ret[10:pos], e.Key)
	copy(ret[pos:pos+vlenInt], e.Value)

	return
}

func (e Entry) Size() int64 {
	totalLen := int64(e.KLen) + int64(e.VLen) + 10
	return totalLen
}

func (e *Entry) SetDelete() {
	e.Extra = ExtraEnum_Delete
}

func decodeEntry(ret []byte) (e Entry) {
	e.KLen = ret[0]
	e.VLen = util.Bytes2Uint32(ret[1:5])
	e.Expire = util.Bytes2Uint32(ret[5:9])
	e.Extra = ExtraEnum(ret[9])
	pos := 10 + int(e.KLen)
	e.Key = ret[10:pos]
	e.Value = ret[pos : pos+int(e.VLen)]
	return e
}

func MustOpen(path string) *File {
	fp, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	return &File{
		fp:     fp,
		offset: util.MustGetFileSize(fp),
		mu:     sync.RWMutex{},
		name:   util.ExtractFileName(path),
	}
}
