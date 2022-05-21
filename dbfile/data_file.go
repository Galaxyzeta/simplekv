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
var md5HasherMu = sync.Mutex{}

const (
	ExtraEnum_Unknown ExtraEnum = iota
	ExtraEnum_Delete
)

// headerLength is the length of entry excludes its key and value.
const headerLength = 21

type Entry struct {
	Checksum    uint32
	LeaderEpoch uint32
	KLen        uint32
	VLen        uint32
	ExpireAt    uint32
	Extra       ExtraEnum
	Key         []byte
	Value       []byte
}

type File struct {
	name                 string       // immutable
	relativePath         string       // immutable
	nameWithoutExtension string       // immutable
	startOffset          int64        // immutable
	fpread               *os.File     // immutable
	fpwrite              *os.File     // immutable
	offset               int64        // critical
	mu                   sync.RWMutex // proctec offset
	readmu               sync.Mutex   // protect read operation
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

// File Deprecated
func (file *File) File(isRead bool) *os.File {
	if isRead {
		return file.fpread
	}
	return file.fpwrite
}

// Return read only file descriptor.
func (file *File) GetFdRead() *os.File {
	return file.fpread
}

// Return append only file descriptor.
func (file *File) GetFdWrite() *os.File {
	return file.fpwrite
}

// Close both read and write file descriptors/
func (file *File) Close() error {
	err1 := file.fpread.Close()
	err2 := file.fpwrite.Close()
	if err1 != nil {
		if err2 != nil {
			return fmt.Errorf("%w, %v", err1, err2.Error())
		}
		return err1
	} else if err2 != nil {
		return err2
	}
	return nil
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
	fp := file.GetFdWrite()
	if _, err = fp.Write(stream); err != nil {
		return
	}
	file.offset += int64(len(stream))
	return
}

func (file *File) Read(offset int64) (e Entry, err error) {
	file.readmu.Lock()
	defer file.readmu.Unlock() // Read operation consists of Seek and Read, not atomic !
	return readSingleEntryAtNoLock(file.fpread, offset)
}

func (file *File) Walk(fn FileWalkFn) {
	fp := file.fpread
	file.readmu.Lock()
	defer file.readmu.Unlock()
	fp.Seek(0, 0)
	for {
		e, err := readSingleEntryNoLock(fp)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		fn(e)
	}
}

func readSingleEntryAtNoLock(fp *os.File, offset int64) (e Entry, err error) {
	fp.Seek(offset, 0)
	return readSingleEntryNoLock(fp)
}

// readSingleEntryNoLock from file, will report if there's an broken data.
func readSingleEntryNoLock(fp *os.File) (e Entry, err error) {
	buffer := make([]byte, headerLength)
	if _, err = fp.Read(buffer); err != nil {
		return
	}
	e = decodeHeader(buffer)
	// Detect malformed data early to prevent speed loss caused by data corruption or totally random decode.
	if e.KLen+e.VLen > uint32(config.DataBlockSize) {
		fmt.Println("entry: ", e.ToString(), "broken data: early detection")
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
		fmt.Println("broken data: validation, key is ", string(e.Key))
		err = config.ErrBrokenData
	}
	return
}

func NewEntry(key string, value string, leaderEpoch int64) (e Entry) {
	// TODO: validate key and value length
	e.LeaderEpoch = uint32(leaderEpoch)
	e.KLen = uint32(len(key))
	e.VLen = uint32(len(value))
	e.Key = []byte(key)
	e.Value = []byte(value)
	e.Checksum = util.Bytes2Uint32(e.calcChecksum())
	return
}

func NewEntryWithAll(key string, value string, leaderEpoch uint32, expireAt uint32, extra ExtraEnum) (e Entry) {
	e.LeaderEpoch = leaderEpoch
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

func (e Entry) ToString() string {
	return fmt.Sprintf("KLen=%d|VLen=%d|Key=%s|Value=%s|", e.KLen, e.VLen, e.Key, e.Value)
}

// Warning: after hashing a lot of data, a panic which says d.nx != 0 will throw.
func (e Entry) calcChecksum() (ret []byte) {
	md5HasherMu.Lock()
	defer md5HasherMu.Unlock()
	defer func() {
		k := recover()
		if k != nil {
			fmt.Println("!!!!!!!!!!!!!!! Md5 HASH Bug detected !!!!!!!!!!!!!!!!")
			fmt.Println("PANIC: ", k)
			md5Hasher = md5.New()
			ret = e.calcChecksum()
		}
	}()
	md5Hasher.Write(e.Encode()[4:])
	val := md5Hasher.Sum(nil)
	md5Hasher.Reset()
	ret = val[len(val)-4:]
	return
}

func (e Entry) Encode() (ret []byte) {
	klenInt := int(e.KLen)
	vlenInt := int(e.VLen)
	totalLen := e.Size()
	ret = make([]byte, totalLen)

	copy(ret[0:4], util.Uint32ToBytes(e.Checksum))
	copy(ret[4:8], util.Uint32ToBytes(e.LeaderEpoch))
	copy(ret[8:12], util.Uint32ToBytes(e.KLen))
	copy(ret[12:16], util.Uint32ToBytes(e.VLen))
	copy(ret[16:20], util.Uint32ToBytes(e.ExpireAt))
	ret[20] = uint8(e.Extra)

	pos := headerLength + klenInt
	copy(ret[headerLength:pos], e.Key)
	copy(ret[pos:pos+vlenInt], e.Value)

	return
}

func (e Entry) Size() int64 {
	totalLen := int64(e.KLen) + int64(e.VLen) + headerLength
	return totalLen
}

func Decode(ret []byte) (e Entry) {
	e = decodeHeader(ret)
	pos := headerLength + int(e.KLen)
	e.Key = ret[headerLength:pos]
	e.Value = ret[pos : pos+int(e.VLen)]
	return e
}

func decodeHeader(ret []byte) (e Entry) {
	e.Checksum = util.Bytes2Uint32(ret[0:4])
	e.LeaderEpoch = util.Bytes2Uint32(ret[4:8])
	e.KLen = util.Bytes2Uint32(ret[8:12])
	e.VLen = util.Bytes2Uint32(ret[12:16])
	e.ExpireAt = util.Bytes2Uint32(ret[16:20])
	e.Extra = ExtraEnum(ret[20])
	return e
}

func MustOpen(path string) *File {
	// Extract file name
	fpwrite := util.MustOpenFileAppend(path)
	fpread := util.MustOpenFileReadonly(path)
	_name := util.ExtractFileName(path)
	_nameWithoutExtension := strings.Split(_name, ".")[0]
	// Parse startOffset
	startOffset, err := strconv.ParseInt(_nameWithoutExtension, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("cannot parse startOffset: %s", err.Error()))
	}

	return &File{
		fpread:               fpread,
		fpwrite:              fpwrite,
		relativePath:         path,
		startOffset:          startOffset,
		offset:               util.MustGetFileSize(fpread),
		mu:                   sync.RWMutex{},
		readmu:               sync.Mutex{},
		name:                 _name,
		nameWithoutExtension: _nameWithoutExtension,
	}
}
