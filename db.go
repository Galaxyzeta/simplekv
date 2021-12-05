package simplekv

import (
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/galaxyzeta/simplekv/cache"
	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/dbfile"
	"github.com/galaxyzeta/simplekv/index"
)

type simpleKV struct {
	files    []*dbfile.File
	mu       sync.RWMutex
	indexer  index.Indexer
	cache    cache.Cacher
	expireAt map[string]uint32
}

var globalKV *simpleKV

// newSimpleKV returns a simplekv but with no files.
func newSimpleKV() *simpleKV {
	return &simpleKV{
		files:    make([]*dbfile.File, 0),
		indexer:  index.NewHashIndexer(), // TODO add option for other indexers
		mu:       sync.RWMutex{},
		expireAt: make(map[string]uint32),
	}
}

func (kv *simpleKV) totalOffset() int64 {
	active := kv.files[len(kv.files)-1]
	return active.Offset() + int64((len(kv.files)-1)*int(config.BlockSize))
}

func (kv *simpleKV) appendNewFile() *dbfile.File {
	if len(kv.files) == 0 {
		newFile := dbfile.MustOpen(fmt.Sprintf("%s/%020d.dat", config.DBDir, 0))
		kv.files = append(kv.files, newFile)
		return newFile
	}
	newFile := dbfile.MustOpen(fmt.Sprintf("%s/%020d.dat", config.DBDir, kv.totalOffset()))
	kv.files = append(kv.files, newFile)
	return newFile
}

func (kv *simpleKV) getActiveFile() *dbfile.File {
	return kv.files[len(kv.files)-1]
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
	return kv.files[idx]
}

func (kv *simpleKV) WriteEntry(e dbfile.Entry) {
	var activeFile *dbfile.File
	activeFile = kv.getActiveFile()
	if activeFile.Offset()+e.Size() > config.BlockSize {
		// need to create a new file
		activeFile = kv.appendNewFile()
	}
	stream := e.Encode()
	activeFile.Write(stream)
}

// MustLoad loads all db files under a certain directory.
func MustLoad() {
	dir := config.DBDir
	globalKV = newSimpleKV()

	// check whether directory exist, if not make a new directory
	if err := os.MkdirAll(config.DBDir, 0644); err != nil {
		panic(err)
	}

	shouldInit := true
	dirWalkFunc := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			panic(err)
		}
		tuple := strings.Split(d.Name(), ".")
		if d.IsDir() || len(tuple) != 2 || tuple[1] != "dat" {
			return nil
		}
		// otherwise it is our desired file
		// TODO check file format
		shouldInit = false
		fullpath := fmt.Sprintf("%s/%s", dir, path)
		fp := dbfile.MustOpen(fullpath)
		internalMustLoad(fp)
		globalKV.files = append(globalKV.files, fp)
		return nil
	}
	fs.WalkDir(os.DirFS(dir), ".", dirWalkFunc)

	if shouldInit {
		// there's no matching db files.
		globalKV.appendNewFile()
	}
}

// internalMustLoad reads the dbfile and buildup index.
// will panic if any error encountered.
func internalMustLoad(f *dbfile.File) {
	// TODO change indexer according to cfg
	indexer := globalKV.indexer

	offset := int64(0)
	filename := f.Name()

	// build up index
	f.Walk(func(e dbfile.Entry) {
		defer func() { offset += e.Size() }()
		strk := string(e.Key)
		switch e.Extra {
		case dbfile.ExtraEnum_Delete:
			indexer.Delete(strk)
			delete(globalKV.expireAt, strk)
		case dbfile.ExtraEnum_Unknown:
			expired := time.Now().Unix() >= int64(e.ExpireAt)
			if e.ExpireAt == 0 { // no expire set
				indexer.Write(strk, index.MakeInode(offset, filename))
			} else if !expired { // not expired
				indexer.Write(strk, index.MakeInode(offset, filename))
				globalKV.expireAt[strk] = e.ExpireAt
			} else { // has expired
				indexer.Delete(strk)
				delete(globalKV.expireAt, strk)
			}
		}
	})

	globalKV.indexer = indexer
}
