package simplekv

import (
	"time"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/dbfile"
	"github.com/galaxyzeta/simplekv/index"
)

// Expire set TTL of a KV entry.
func Expire(key string, expire int) error {

	// TODO change this to Exist()
	val, err := Get(key)
	if err != nil {
		return err // record not found or other err
	}

	// write an expire record
	if err := setVal(key, val, uint32(expire), dbfile.ExtraEnum_Unknown); err != nil {
		return err
	}

	// set in-mem expire map
	globalKV.mu.Lock()
	defer globalKV.mu.Unlock()
	globalKV.expire[key] = uint32(time.Now().Add(time.Second * time.Duration(expire)).Unix())

	return nil
}

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
	globalKV.mu.RLock()
	defer globalKV.mu.RUnlock()
	val, ok := globalKV.expire[key]
	return val, ok
}

// tryDeleteCache deletes the cache if cache is not nil.
func tryDeleteCache(key string) {
	if globalKV.cache != nil {
		globalKV.cache.Delete(key)
	}
}

// delEntry deletes the expire map, cache and indexer with given key.
// This function contain lock operation.
func delEntry(key string) {
	globalKV.mu.Lock()
	defer globalKV.mu.Unlock()
	delete(globalKV.expire, key)
	globalKV.indexer.Delete(key)
	tryDeleteCache(key)
}

// setVal writes entry to disk, set indexer and cache as well.
// Notice that expire is represented in second.
func setVal(key string, val string, expire uint32, extra dbfile.ExtraEnum) error {
	// try to override expire with exist expire time.
	expire0, ok := getExpiredAt(key)
	if expire == 0 && ok && !checkExpire(expire0) {
		// no expire to set and has not expired => override expire time
		expire = expire0
	}

	// encode entry to stream
	e := dbfile.NewEntryWithExpire(key, val, expire)
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

func Exist(key string) bool {
	globalKV.mu.RLock()
	defer globalKV.mu.RUnlock()
	return internalExist(key)
}

// internalExist checks whether the key exist without lock.
func internalExist(key string) bool {
	_, ok := globalKV.indexer.Read(key)
	return ok
}

func TTL(key string) (uint32, error) {
	globalKV.mu.RLock()
	defer globalKV.mu.RUnlock()
	return internalTTL(key)
}

// internalTTL returns TTL without lock.
// Return error when the key does not exist, or expiration has not been set.
func internalTTL(key string) (uint32, error) {
	ok := internalExist(key)
	if !ok {
		return 0, config.ErrRecordNotFound
	}
	if val, ok := globalKV.expire[key]; ok {
		if diff := uint32(int64(val) - time.Now().Unix()); diff > 0 {
			return diff, nil
		}
	}
	return 0, config.ErrNoRelatedExpire
}

func Get(key string) (string, error) {

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

// Write writes to the disk file and then update the index and the cache.
func Write(key string, value string) error {
	return setVal(key, value, 0, dbfile.ExtraEnum_Unknown)
}

func Delete(key string) error {

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
