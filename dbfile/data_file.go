package dbfile

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/util"
)

type ExtraEnum uint8

var md5Hasher = md5.New()

const (
	ExtraEnum_Unknown ExtraEnum = iota
	ExtraEnum_Delete
)

type Entry struct {
	Checksum uint32
	KLen     uint32
	VLen     uint32
	ExpireAt uint32
	Extra    ExtraEnum
	Key      []byte
	Value    []byte
}

type File struct {
	name                 string   // immutable
	relativePath         string   // immutable
	nameWithoutExtension string   // immutable
	startOffset          int64    // immutable
	fp                   *os.File // immutable
	offset               int64    // critical
	mu                   sync.RWMutex
}

type FileWalkFn func(e Entry)

func (file *File) Offset() int64 {
	file.mu.RLock()
	defer file.mu.RUnlock()
	return file.offset
}

func (file *File) SetOffset(val int64) {
	file.mu.RLock()
	defer file.mu.RUnlock()
	file.offset = val
}

func (file *File) File() *os.File {
	file.mu.RLock()
	defer file.mu.RUnlock()
	return file.fp
}

func (file *File) Name() string {
	return file.name
}

func (file *File) RelativePath() string {
	return file.relativePath
}

func (file *File) StartOffsetInt64() int64 {
	return file.startOffset
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

// readSingleEntry from file, will report if there's an broken data.
func readSingleEntry(fp *os.File) (e Entry, err error) {
	buffer := make([]byte, 17)
	if _, err = fp.Read(buffer); err != nil {
		return
	}
	e = decodeHeader(buffer)
	// Detect malformed data early to prevent speed loss caused by data corruption or totally random decode.
	if e.KLen+e.VLen > uint32(config.DataBlockSize) {
		err = config.ErrBrokenData
		return
	}

	buffer = make([]byte, int(e.KLen)+int(e.VLen))
	_, err = fp.Read(buffer)
	if err != nil {
		return
	}
	e.Key = buffer[0:e.KLen]
	e.Value = buffer[e.KLen:]
	if !e.Validate() {
		err = config.ErrBrokenData
	}
	return
}

func NewEntry(key string, value string) (e Entry) {
	// TODO: validate key and value length
	e.KLen = uint32(len(key))
	e.VLen = uint32(len(value))
	e.Key = []byte(key)
	e.Value = []byte(value)
	e.Checksum = util.Bytes2Uint32(e.calcChecksum())
	return
}

func NewEntryWithAll(key string, value string, expireAt uint32, extra ExtraEnum) (e Entry) {
	e.KLen = uint32(len(key))
	e.VLen = uint32(len(value))
	e.Key = []byte(key)
	e.Value = []byte(value)
	e.ExpireAt = expireAt
	e.Extra = extra
	e.Checksum = util.Bytes2Uint32(e.calcChecksum())
	return
}

// Validate the checksum to see whether they match.
func (e Entry) Validate() bool {
	checkSum := e.calcChecksum()
	for i, eachByte := range util.Uint32ToBytes(e.Checksum) {
		if checkSum[i] != eachByte {
			return false
		}
	}
	return true
}

func (e Entry) calcChecksum() []byte {
	md5Hasher.Write(e.Encode()[4:])
	val := md5Hasher.Sum(nil)
	md5Hasher.Reset()
	return val[len(val)-4:]
}

func (e Entry) Encode() (ret []byte) {
	klenInt := int(e.KLen)
	vlenInt := int(e.VLen)
	totalLen := e.Size()
	ret = make([]byte, totalLen)

	copy(ret[0:4], util.Uint32ToBytes(e.Checksum))
	copy(ret[4:8], util.Uint32ToBytes(e.KLen))
	copy(ret[8:12], util.Uint32ToBytes(e.VLen))
	copy(ret[12:16], util.Uint32ToBytes(e.ExpireAt))
	ret[16] = uint8(e.Extra)

	pos := 17 + klenInt
	copy(ret[17:pos], e.Key)
	copy(ret[pos:pos+vlenInt], e.Value)

	return
}

func (e Entry) Size() int64 {
	totalLen := int64(e.KLen) + int64(e.VLen) + 17
	return totalLen
}

func Decode(ret []byte) (e Entry) {
	e = decodeHeader(ret)
	pos := 17 + int(e.KLen)
	e.Key = ret[17:pos]
	e.Value = ret[pos : pos+int(e.VLen)]
	return e
}

func decodeHeader(ret []byte) (e Entry) {
	e.Checksum = util.Bytes2Uint32(ret[0:4])
	e.KLen = util.Bytes2Uint32(ret[4:8])
	e.VLen = util.Bytes2Uint32(ret[8:12])
	e.ExpireAt = util.Bytes2Uint32(ret[12:16])
	e.Extra = ExtraEnum(ret[16])
	return e
}

func MustOpen(path string) *File {
	// Extract file name
	fp := util.MustOpenFileAppend(path)
	_name := util.ExtractFileName(path)
	_nameWithoutExtension := strings.Split(_name, ".")[0]
	// Parse startOffset
	startOffset, err := strconv.ParseInt(_nameWithoutExtension, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("cannot parse startOffset: %s", err.Error()))
	}

	return &File{
		fp:                   fp,
		relativePath:         path,
		startOffset:          startOffset,
		offset:               util.MustGetFileSize(fp),
		mu:                   sync.RWMutex{},
		name:                 _name,
		nameWithoutExtension: _nameWithoutExtension,
	}
}
