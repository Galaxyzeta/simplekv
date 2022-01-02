package dbfile

import (
	"io"
	"os"
	"sync"

	"github.com/galaxyzeta/simplekv/util"
)

type VarFile struct {
	rwmu sync.RWMutex
	hwm  int64
	fp   *os.File
}

// MustOpenVar opens file and reads all status variables into memory.
func MustOpenVar(path string) (ret *VarFile) {
	fp := util.MustOpenFileRndRW(path)
	waterMark := make([]byte, 8)
	_, err := fp.Read(waterMark)
	if err != nil && err != io.EOF {
		panic(err)
	}
	var hwm int64
	if len(waterMark) != 0 {
		hwm = int64(util.Bytes2Uint64(waterMark))
	}
	return &VarFile{
		hwm: hwm,
		fp:  fp,
	}
}

// OverwriteWatermark overwrites the watermark value in the file.
func (wm *VarFile) OverwriteWatermark(hwm int64) error {
	wm.rwmu.Lock()
	defer wm.rwmu.Unlock()
	if wm.hwm == hwm {
		return nil
	}
	wm.hwm = hwm
	if _, err := wm.fp.WriteAt(util.Uint64ToBytes(uint64(wm.hwm)), 0); err != nil {
		return err
	}
	return nil
}

func (wm *VarFile) ReadWatermarkFromCache() int64 {
	wm.rwmu.RLock()
	defer wm.rwmu.RUnlock()
	return wm.hwm
}
