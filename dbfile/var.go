package dbfile

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/galaxyzeta/simplekv/util"
)

type LeaderEpochAndOffset struct {
	LeaderEpoch int64
	Offset      int64
}

func (leo LeaderEpochAndOffset) Encode() []byte {
	return []byte(fmt.Sprintf("%d %d\n", leo.LeaderEpoch, leo.Offset))
}

func makeLeaderEpochOffset(leaderEpoch int64, offset int64) LeaderEpochAndOffset {
	return LeaderEpochAndOffset{
		LeaderEpoch: leaderEpoch,
		Offset:      offset,
	}
}

type Var struct {
	rwmu                  sync.RWMutex
	hwm                   int64
	leaderEpochAndOffsets []LeaderEpochAndOffset
	fps                   VarFileCollection
}

type VarFileCollection struct {
	hwm               *os.File
	leaderEpochOffset *os.File
}

// MustOpenVar opens highWatermark checkpoint and leaderEpochOffset checkpoint files, read their contents and load into memory.
func MustOpenVar(hwmPath, leaderEpochOffsetPath string) (ret *Var) {
	varfps := VarFileCollection{
		hwm:               util.MustOpenFileRndRW(hwmPath),
		leaderEpochOffset: util.MustOpenFileAppend(leaderEpochOffsetPath),
	}
	return &Var{
		rwmu:                  sync.RWMutex{},
		hwm:                   internalMustParseHwm(varfps.hwm),
		leaderEpochAndOffsets: internalMustParseLeaderEpochOffsetFile(varfps.leaderEpochOffset),
		fps:                   varfps,
	}
}

func (v *Var) Shutdown() error {
	if err := v.fps.hwm.Close(); err != nil {
		return err
	}
	if err := v.fps.leaderEpochOffset.Close(); err != nil {
		return err
	}
	return nil
}

func (v *Var) internalPersistLeaderEpochOffset(data LeaderEpochAndOffset) error {
	_, err := v.fps.leaderEpochOffset.Write(data.Encode())
	return err
}

func internalMustParseHwm(file *os.File) int64 {
	waterMark := make([]byte, 8)
	_, err := file.Read(waterMark)
	if err != nil && err != io.EOF {
		panic(err)
	}
	if len(waterMark) != 0 {
		return int64(util.Bytes2Uint64(waterMark))
	}
	return 0
}

func internalMustParseLeaderEpochOffsetFile(file *os.File) []LeaderEpochAndOffset {
	content, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}
	vectors := strings.Split(string(content), "\n")
	ret := make([]LeaderEpochAndOffset, 0, len(vectors))
	mustConv2Int64 := func(strval string) int64 {
		intval, err := strconv.Atoi(strval)
		if err != nil {
			panic(err)
		}
		return int64(intval)
	}
	for _, vector := range vectors {
		if util.StringBlank(vector) {
			continue
		}
		pair := strings.Split(vector, " ")
		if len(pair) != 2 {
			panic("the length of pair is not 2")
		}
		ret = append(ret, LeaderEpochAndOffset{
			LeaderEpoch: mustConv2Int64(pair[0]),
			Offset:      mustConv2Int64(pair[1]),
		})
	}
	return ret
}

// AppendLeaderEpochAndOffsetVector persist leaderEpoch and offset to file and update the cache
func (v *Var) AppendLeaderEpochAndOffsetVector(leaderEpoch int64, offset int64) error {
	data := makeLeaderEpochOffset(leaderEpoch, offset)
	if err := v.internalPersistLeaderEpochOffset(data); err != nil {
		return err
	}
	v.leaderEpochAndOffsets = append(v.leaderEpochAndOffsets, data)
	return nil
}

// CloneLeaderEpochAndOffset returns a clone of []leaderEpochAndOffset
func (v *Var) CloneLeaderEpochAndOffset() []LeaderEpochAndOffset {
	v.rwmu.RLock()
	defer v.rwmu.RUnlock()
	ret := make([]LeaderEpochAndOffset, len(v.leaderEpochAndOffsets))
	copy(ret[:], v.leaderEpochAndOffsets[:])
	return ret
}

// Get lastest leaderEpoch it knows.
func (v *Var) GetLatestLeaderEpoch() int64 {
	v.rwmu.RLock()
	defer v.rwmu.RUnlock()
	if len(v.leaderEpochAndOffsets) == 0 {
		return 0
	}
	return v.leaderEpochAndOffsets[len(v.leaderEpochAndOffsets)-1].LeaderEpoch
}

func (v *Var) ValidateLeaderEpochAndOffset() bool {
	v.rwmu.RLock()
	defer v.rwmu.RUnlock()
	if len(v.leaderEpochAndOffsets) <= 1 {
		return true
	}
	cmpLeaderEpoch := v.leaderEpochAndOffsets[0].LeaderEpoch
	cmpOffset := v.leaderEpochAndOffsets[0].Offset
	for i := 1; i < len(v.leaderEpochAndOffsets); i++ {
		elem := v.leaderEpochAndOffsets[i]
		if elem.LeaderEpoch < cmpLeaderEpoch || elem.Offset < cmpOffset {
			return false
		}
		cmpLeaderEpoch = elem.LeaderEpoch
		cmpOffset = elem.Offset
	}
	return true
}

// OverwriteWatermark overwrites the watermark value in the file.
func (v *Var) OverwriteWatermark(hwm int64) error {
	v.rwmu.Lock()
	defer v.rwmu.Unlock()
	if v.hwm == hwm {
		return nil
	}
	v.hwm = hwm
	if _, err := v.fps.hwm.WriteAt(util.Uint64ToBytes(uint64(v.hwm)), 0); err != nil {
		return err
	}
	return nil
}

func (wm *Var) ReadWatermarkFromCache() int64 {
	wm.rwmu.RLock()
	defer wm.rwmu.RUnlock()
	return wm.hwm
}
