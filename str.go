package simplekv

import (
	"time"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/dbfile"
	"github.com/galaxyzeta/simplekv/index"
)

// hasExpired checks whether the entry related with given key has expired.
func hasExpired(key string) bool {
	expireAt, ok := getExpiredAt(key)
	if !ok {
		return false
	}
	return checkExpire(expireAt)
}

// checkExpire checks whether time.Now() > expireAt
func checkExpire(expireAt uint32) bool {
	return uint32(time.Now().Unix()) >= expireAt
}

// getExpiredAt with lock.
func getExpiredAt(key string) (uint32, bool) {
	val, ok := globalKV.expireAt[key]
	return val, ok
}

// tryDeleteCache deletes the cache if cache is not nil.
func tryDeleteCache(key string) {
	if globalKV.cache != nil {
		globalKV.cache.Delete(key)
	}
}

// delEntry deletes the expire map, cache and indexer with given key.
func delEntry(key string) {
	delete(globalKV.expireAt, key)
	globalKV.indexer.Delete(key)
	tryDeleteCache(key)
}

func getVal(key string) (string, error) {
	// check expire
	if hasExpired(key) {
		delEntry(key)
		return "", config.ErrRecordExpired
	}

	// try to get from cache
	if globalKV.cache != nil {
		val, ok := globalKV.cache.Load(key)
		if ok {
			return val.(string), nil
		}
	}

	// cache miss, get index and lookup disk
	inode, ok := globalKV.indexer.Read(key)
	if !ok {
		return "", config.ErrRecordNotFound
	}

	f := globalKV.getFileByID(inode.FileID)
	if f == nil {
		return "", config.ErrFileNotFound
	}

	e, err := f.Read(inode.Offset)
	if err != nil {
		return "", err
	}

	return string(e.Value), nil
}

// setVal writes entry to disk, set indexer and cache as well.
// Notice that expire is represented in second.
func setVal(key string, val string, expireAt uint32, extra dbfile.ExtraEnum) error {

	// encode entry to stream
	e := dbfile.NewEntryWithExpire(key, val, expireAt)
	e.Extra = extra
	stream := e.Encode()

	// choose file
	f := globalKV.getActiveFile()
	if f.Offset()+int64(len(stream)) > config.BlockSize {
		f = globalKV.appendNewFile()
	}

	// write to disk
	curOffset := f.Offset()
	if err := f.Write(e.Encode()); err != nil {
		return err
	}

	// update index and cache
	globalKV.indexer.Write(key, index.MakeInode(curOffset, f.Name()))
	if globalKV.cache != nil {
		globalKV.cache.Store(key, val)
	}
	return nil
}

// internalExist checks whether the key exist without lock.
func internalExist(key string) bool {
	_, ok := globalKV.indexer.Read(key)
	return ok
}

// internalTTL returns TTL without lock.
// Return error when the key does not exist, or expiration has not been set.
func internalTTL(key string) (uint32, error) {
	ok := internalExist(key)
	if !ok {
		return 0, config.ErrRecordNotFound
	}
	if val, ok := globalKV.expireAt[key]; ok {
		if diff := uint32(int64(val) - time.Now().Unix()); diff > 0 {
			return diff, nil
		}
	}
	return 0, config.ErrNoRelatedExpire
}

// Expire set TTL of a KV entry.
func Expire(key string, ttl int) error {

	globalKV.mu.Lock()
	defer globalKV.mu.Unlock()

	val, err := getVal(key)
	if err != nil {
		return err // record not found or other err
	}

	// calc expireAt
	var expireAt uint32
	var hadSetExpire bool
	if ttl != 0 {
		expireAt = uint32(time.Now().Unix()) + uint32(ttl)
	} else {
		expireAt, hadSetExpire = getExpiredAt(key)
		if hadSetExpire && checkExpire(expireAt) {
			expireAt = 0
		}
	}

	// write an expire record
	if err := setVal(key, val, expireAt, dbfile.ExtraEnum_Unknown); err != nil {
		return err
	}

	// update expireAt
	if ttl != 0 {
		globalKV.expireAt[key] = expireAt
	} else if hadSetExpire && expireAt == 0 {
		delete(globalKV.expireAt, key) // had set expireAt but has already expired.
	}

	return nil
}

func Exist(key string) bool {
	globalKV.mu.RLock()
	defer globalKV.mu.RUnlock()
	return internalExist(key)
}

func TTL(key string) (uint32, error) {
	globalKV.mu.RLock()
	defer globalKV.mu.RUnlock()
	return internalTTL(key)
}

func Get(key string) (string, error) {
	globalKV.mu.RLock()
	defer globalKV.mu.RUnlock()
	return getVal(key)
}

// Write writes to the disk file and then update the index and the cache.
func Write(key string, value string) error {
	globalKV.mu.Lock()
	defer globalKV.mu.Unlock()
	return setVal(key, value, 0, dbfile.ExtraEnum_Unknown)
}

func Delete(key string) error {
	globalKV.mu.Lock()
	defer globalKV.mu.Unlock()

	if err := setVal(key, "", 0, dbfile.ExtraEnum_Delete); err != nil {
		return err
	}

	// write disk
	f := globalKV.getActiveFile()
	e := dbfile.NewEntry(key, "")
	e.SetDelete()
	if err := f.Write(e.Encode()); err != nil {
		return err
	}

	// delete index and cache
	globalKV.indexer.Delete(key)
	if globalKV.cache != nil {
		globalKV.cache.Delete(key)
	}

	return nil
}
