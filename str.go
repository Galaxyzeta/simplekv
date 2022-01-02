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

func getExpiredAt(key string) (uint32, bool) {
	val, ok := dataInstance.expireAt[key]
	return val, ok
}

// tryDeleteCache deletes the cache if cache is not nil.
func tryDeleteCache(key string) {
	if dataInstance.cache != nil {
		dataInstance.cache.Delete(key)
	}
}

// delEntry deletes the expire map, cache and indexer with given key.
func delEntry(key string) {
	delete(dataInstance.expireAt, key)
	dataInstance.indexer.Delete(key)
	tryDeleteCache(key)
}

func getVal(key string) (string, error) {
	// check expire
	if hasExpired(key) {
		delEntry(key)
		return "", config.ErrRecordExpired
	}

	// try to get from cache
	if dataInstance.cache != nil {
		val, ok := dataInstance.cache.Load(key)
		if ok {
			return val.(string), nil
		}
	}

	// cache miss, get index and lookup disk
	inode, ok := dataInstance.indexer.Read(key)
	if !ok {
		return "", config.ErrRecordNotFound
	}

	f := dataInstance.getFileByID(inode.FileID)
	if f == nil {
		return "", config.ErrFileNotFound
	}

	e, err := f.Read(inode.Offset)
	if err != nil {
		return "", err
	}

	return string(e.Value), nil
}

// setValWithLock writes entry to disk, set indexer, cache, expiration as well.
// Lock operation is contained in this method.
// Notice that expire is represented in second.
func setValWithLock(key string, val string, expireAt uint32, extra dbfile.ExtraEnum) error {

	dataInstance.mu.Lock()
	defer dataInstance.mu.Unlock()

	// if this is a delete operation, pre-check whether the item exist.
	if extra == dbfile.ExtraEnum_Delete && !internalExist(key) {
		return nil
	}
	// if expireAt is zero, try to override expireAt.
	if expireAt == 0 {
		_expireAt, _ := getExpiredAt(key)
		if !checkExpire(_expireAt) {
			expireAt = _expireAt
		}
	}

	// encode entry to stream
	e := dbfile.NewEntryWithAll(key, val, expireAt, extra)
	e.Extra = extra

	// using write proxy to handle data commit related problems.
	infileOffsetBeforeWrite, whichFile, err := writeProxy(e)
	if err != nil {
		dataInstance.logger.Errorf("Failed to commit: %s", err.Error())
		return err
	}
	applyEntry(e, infileOffsetBeforeWrite, whichFile)
	return nil
}

func applyEntry(e dbfile.Entry, offsetBeforeWrite int64, whichFile *dbfile.File) {
	// update index and cache
	key := string(e.Key)
	val := string(e.Value)
	if e.Extra == dbfile.ExtraEnum_Unknown {
		// Set
		dataInstance.indexer.Write(key, index.MakeInode(offsetBeforeWrite, whichFile.Name()))
		if dataInstance.cache != nil {
			dataInstance.cache.Store(key, val)
		}
	} else {
		// Delete
		dataInstance.indexer.Delete(key)
		if dataInstance.cache != nil {
			dataInstance.cache.Delete(key)
		}
	}

	// handle expire
	if e.ExpireAt != 0 {
		dataInstance.expireAt[key] = e.ExpireAt
	}
}

// internalExist checks whether the key exist without lock.
func internalExist(key string) bool {
	_, ok := dataInstance.indexer.Read(key)
	return ok
}

// internalTTL returns TTL without lock.
// Return error when the key does not exist, or expiration has not been set.
func internalTTL(key string) (uint32, error) {
	ok := internalExist(key)
	if !ok {
		return 0, config.ErrRecordNotFound
	}
	if val, ok := dataInstance.expireAt[key]; ok {
		if diff := uint32(int64(val) - time.Now().Unix()); diff > 0 {
			return diff, nil
		}
	}
	return 0, config.ErrNoRelatedExpire
}

// Expire set TTL of a KV entry.
func Expire(key string, ttl int) error {

	dataInstance.mu.Lock()
	defer dataInstance.mu.Unlock()

	val, err := getVal(key)
	if err != nil {
		return err // record not found or other err
	}

	// calc expireAt
	expireAt := uint32(time.Now().Unix()) + uint32(ttl)

	// var expireAt uint32
	// var hadSetExpire bool
	// if ttl != 0 {
	// 	expireAt = uint32(time.Now().Unix()) + uint32(ttl)
	// } else {
	// 	expireAt, hadSetExpire = getExpiredAt(key)
	// 	if hadSetExpire && checkExpire(expireAt) {
	// 		expireAt = 0
	// 	}
	// }

	// write an expire record
	return setValWithLock(key, val, expireAt, dbfile.ExtraEnum_Unknown)

	// update expireAt
	// if ttl != 0 {
	// 	dataInstance.expireAt[key] = expireAt
	// } else if hadSetExpire && expireAt == 0 {
	// 	delete(dataInstance.expireAt, key) // had set expireAt but has already expired.
	// }
}

func Exist(key string) bool {
	dataInstance.mu.RLock()
	defer dataInstance.mu.RUnlock()
	return internalExist(key)
}

func TTL(key string) (uint32, error) {
	dataInstance.mu.RLock()
	defer dataInstance.mu.RUnlock()
	return internalTTL(key)
}

func Get(key string) (string, error) {
	dataInstance.mu.RLock()
	defer dataInstance.mu.RUnlock()
	return getVal(key)
}

// Write writes to the disk file and then update the index and the cache.
func Write(key string, value string) error {
	return setValWithLock(key, value, 0, dbfile.ExtraEnum_Unknown)
}

func Delete(key string) error {
	return setValWithLock(key, "", 0, dbfile.ExtraEnum_Delete)
}
