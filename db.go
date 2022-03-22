package simplekv

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/galaxyzeta/simplekv/cache"
	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/dbfile"
	"github.com/galaxyzeta/simplekv/index"
	"github.com/galaxyzeta/simplekv/util"
)

type simpleKvState byte

const (
	simpleKvState_Inited simpleKvState = iota
	simpleKvState_Booted
	simpleKvState_Ready
	simpleKvState_ShuttingDown
	simplekvState_Closed simpleKvState = iota
)

type simpleKV struct {
	files    []*dbfile.File // write-ahead log files.
	vars     *dbfile.Var    //
	mu       sync.RWMutex
	indexer  index.Indexer
	cache    cache.Cacher
	expireAt map[string]uint32
	logger   *util.Logger
	state    atomic.Value
}

var dataInstance *simpleKV

func (kv *simpleKV) isReady() bool {
	return kv.state.Load().(simpleKvState) == simpleKvState_Ready
}

func (kv *simpleKV) internalSetState(newState simpleKvState) {
	kv.state.Store(newState)
}

func (kv *simpleKV) internalGetState() simpleKvState {
	return kv.state.Load().(simpleKvState)
}

func (kv *simpleKV) internalTotalOffset() int64 { // TODO may panic
	active := kv.files[len(kv.files)-1]
	return active.Offset() + active.StartOffsetInt64()
}

func (kv *simpleKV) totalOffset() int64 { // TODO may panic
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.internalTotalOffset()
}

func (kv *simpleKV) appendNewFile() *dbfile.File {
	if len(kv.files) == 0 {
		newFile := dbfile.MustOpen(fmt.Sprintf("%s/%020d.dat", config.DataDir, 0))
		kv.files = append(kv.files, newFile)
		return newFile
	}
	newFile := dbfile.MustOpen(fmt.Sprintf("%s/%020d.dat", config.DataDir, kv.internalTotalOffset()))
	kv.files = append(kv.files, newFile)
	return newFile
}

func (kv *simpleKV) getActiveFile() *dbfile.File {
	return kv.files[len(kv.files)-1]
}

func (kv *simpleKV) getNextActiveFile(stream []byte) *dbfile.File {
	file := dataInstance.getActiveFile()
	if file.Offset()+int64(len(stream)) > config.DataBlockSize {
		file = dataInstance.appendNewFile()
	}
	return file
}

// getFileByID return the matching file according to given id.
// will return nil if not found.
func (kv *simpleKV) getFileByID(idStr string) *dbfile.File {
	idx := sort.Search(len(kv.files), func(i int) bool {
		return kv.files[i].Name() >= idStr
	})
	if idx == len(kv.files) {
		return nil
	}
	f := kv.files[idx]
	if f.Name() == idStr {
		return f
	}
	return nil
}

func (kv *simpleKV) findFileMightContainOffset(offset int64) (*dbfile.File, int, error) {
	// Find smallest index tfhat makes the file's start offset bigger than the provided offset.
	// The target file index is the search result minus 1.
	idx := sort.Search(len(kv.files), func(i int) bool { // TODO switch this to own designed alghorithm
		return kv.files[i].StartOffsetInt64() > offset
	})
	// Check whether the offset has a possibility of containing in the last file.
	if idx == len(kv.files) && kv.files[idx-1].StartOffsetInt64()+config.DataBlockSize < offset {
		return nil, 0, config.ErrFileNotFound
	}
	idx--
	if idx < 0 {
		idx = 0
	}
	return kv.files[idx], idx, nil
}

// readEntries reads the entry from given total offset, and read for $count records.
// This function might return the following errors:
// - io.EOF: Has read to the enf of the last file.
// - config.ErrFileNotFound: The file which contains the starting offset is not found.
// - config.ErrBrokenData: The entry checksum validation has failed due to either disk corruption or wrong starting offset.
func (kv *simpleKV) readEntries(from int64, count int64) (ret [][]byte, err error) {
	if count == 0 {
		return
	}
	var currentGlobalOffset = from
	var entry dbfile.Entry
	// First get the file that contains given offset.
	currentFile, idx, err := kv.findFileMightContainOffset(currentGlobalOffset)
	if err != nil {
		return
	}
	for count > 0 {
		entry, err = currentFile.Read(currentGlobalOffset - currentFile.StartOffsetInt64())
		if err != nil {
			if err == io.EOF {
				// no more files to read, try to read from commitManager.
				if idx == len(kv.files)-1 {
					return
				}
				idx++ // move on to the next file.
				currentFile = kv.files[idx]
				continue
			} else {
				return // real error
			}
		}
		ret = append(ret, entry.Encode())
		currentGlobalOffset += entry.Size()
		count -= 1
	}
	return
}

// truncateEntries drops all entries which offset is higher than highOffset.
// This function must be performed before replaying the log to build up index.
func (kv *simpleKV) truncateEntries(highOffset int64) error {
	file, idx, err := kv.findFileMightContainOffset(highOffset)
	if err != nil {
		kv.logger.Errorf("find file err: %s", err.Error())
		return err
	}
	// close and delete remaining logs
	for i := len(kv.files) - 1; i > idx; i-- {
		osfile := file.File()
		if err = osfile.Close(); err != nil {
			kv.logger.Errorf("close file err: %s", err.Error())
			return err
		}
		if err = os.Remove(file.RelativePath()); err != nil {
			kv.logger.Errorf("remove file err: %s", err.Error())
			return err
		}
	}
	// validate a possible read, then truncate the file by offset
	if _, err := file.Read(highOffset); err != nil && err != io.EOF {
		kv.logger.Errorf("read entry err: %s", err.Error())
		return err
	}
	truncatedOffset := highOffset - file.StartOffsetInt64()
	if err := os.Truncate(file.RelativePath(), truncatedOffset); err != nil {
		kv.logger.Errorf("truncate file err: %s", err.Error())
		return err
	}
	file.SetOffset(truncatedOffset)
	// OK
	return nil
}

// initDataPlaneSingleton loads all db files under a certain directory.
func initDataPlaneSingleton() {
	dataInstance = &simpleKV{
		files:    make([]*dbfile.File, 0),
		indexer:  index.NewHashIndexer(), // TODO add option for other indexers
		mu:       sync.RWMutex{},
		expireAt: make(map[string]uint32),
		logger:   util.NewLogger("[SimpleKV]", config.LogOutputWriter),
		state:    atomic.Value{},
	}
	dataInstance.internalSetState(simpleKvState_Inited)
}

// internalMustLoad reads the dbfile and buildup index.
// Will panic if any error encountered.
func internalMustLoad(f *dbfile.File) {
	// TODO change indexer according to cfg
	indexer := dataInstance.indexer

	offset := int64(0)
	filename := f.Name()

	// build up index
	f.Walk(func(e dbfile.Entry) {
		defer func() { offset += e.Size() }()
		strk := string(e.Key)
		switch e.Extra {
		case dbfile.ExtraEnum_Delete:
			indexer.Delete(strk)
			delete(dataInstance.expireAt, strk)
		case dbfile.ExtraEnum_Unknown:
			expired := time.Now().Unix() >= int64(e.ExpireAt)
			if e.ExpireAt == 0 { // no expire set
				indexer.Write(strk, index.MakeInode(offset, filename))
			} else if !expired { // not expired
				indexer.Write(strk, index.MakeInode(offset, filename))
				dataInstance.expireAt[strk] = e.ExpireAt
			} else { // has expired
				indexer.Delete(strk)
				delete(dataInstance.expireAt, strk)
			}
		}
	})

	dataInstance.indexer = indexer
}

// Pre-read data file metadata into memory, but do not build up index.
// FileIdx and ShouldDoInternalLoad are control parameters of this closure.
func internalReadDbFiles(dirFs fs.FS, shouldDoInternalLoad bool) {
	var fileIdx = 0
	fn := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			panic(err)
		}
		tuple := strings.Split(d.Name(), ".")
		if d.IsDir() || len(tuple) != 2 || tuple[1] != "dat" {
			return nil
		}
		// If the file has been opened, do not open it again.
		var fp *dbfile.File
		if len(dataInstance.files) <= fileIdx {
			fullpath := fmt.Sprintf("%s/%s", config.DataDir, path)
			fp = dbfile.MustOpen(fullpath)
			dataInstance.files = append(dataInstance.files, fp)
		} else {
			fp = dataInstance.files[fileIdx]
		}
		if shouldDoInternalLoad {
			internalMustLoad(fp)
		}
		fileIdx++
		return nil
	}
	fs.WalkDir(dirFs, ".", fn)
}

func startDataPlaneSingleton() {

	if dataInstance.internalGetState() != simpleKvState_Inited {
		dataInstance.logger.Fatalf("cannot start dataPlaneSingleton before you init it")
	}

	// Check whether directory exist, if not make a new directory
	util.MustDo(func() error { return os.MkdirAll(config.DataDir, 0644) })

	// Open var file to read high-watermark.
	dataInstance.vars = dbfile.MustOpenVar(
		fmt.Sprintf("%s/%s", config.DataDir, "var"),
		fmt.Sprintf("%s/%s", config.DataDir, "leaderEpochOffset")) // TODO magic string elimination

	// Load file metadata, but do not build up index.
	var dirFs = os.DirFS(config.DataDir)
	internalReadDbFiles(dirFs, false)
	if len(dataInstance.files) == 0 {
		// Initial use of db, thus no any files. If this condition occur, should append new file
		// Else, totalOffset() call will panic.
		dataInstance.appendNewFile()
	}

	// Now that we obtained fuzzy file offsets, wait until leader is elected.
	// After that, ask the leader for its HWM and truncate the log according to it.
	for {
		ctrlInstance.condHasLeader.LoopWaitUntilTrue()
		leaderHostport, ok := ctrlInstance.currentLeaderHostport()
		if !ok {
			dataInstance.logger.Errorf("Failed to get current leader hostport. Retry for another round.")
			time.Sleep(config.RetryBackoff)
			continue
		}
		leaderEpochAndOffset, err := ctrlInstance.rpcMgr.collectLeaderEpochOffset(context.Background(), leaderHostport, dataInstance.vars.GetLatestLeaderEpoch())
		if err != nil {
			dataInstance.logger.Errorf("Failed to collect leaderEpochAndOffset from leader: %s Retry again.", err.Error())
			time.Sleep(config.RetryBackoff)
			continue
		}
		// LeaderEpoch is obtained, now truncate the log.
		err = dataInstance.truncateEntries(leaderEpochAndOffset.Offset)
		if err != nil {
			dataInstance.logger.Fatalf(err.Error())
		}
		break
	}

	// Log is truncated, now build up index anc cache into memory.
	internalReadDbFiles(dirFs, true)
	dataInstance.state.Store(simpleKvState_Ready)
}

// shutdownGracefullty release all related resources.
func (kv *simpleKV) shutdownGracefully() {
	kv.internalSetState(simpleKvState_ShuttingDown)
	for _, fp := range kv.files {
		err := fp.File().Close()
		if err != nil {
			kv.logger.Errorf("failed to close file %s, err: %s", fp.Name(), err.Error())
		}
	}
	if err := kv.vars.Shutdown(); err != nil {
		kv.logger.Errorf("failed to shutdown var files, err: %s", err.Error())
	}
	kv.internalSetState(simplekvState_Closed)
	dataInstance = nil
	fmt.Println(">>>> DataPlane shutdown OK")
}
