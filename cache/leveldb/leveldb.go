package leveldb

import (
	"time"

	"github.com/golang/glog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/zvelo/ttlru"
)

// Cache - LevelDB cache.
type Cache struct {
	db  *leveldb.DB
	ttl time.Duration
	lc  *ttlru.Cache
}

// Open - Opens LevelDB cache.
func Open(path string, ttl time.Duration) (cache *Cache, err error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return
	}
	// local cache for short call lookup
	lc := ttlru.New(1000, time.Minute)
	return &Cache{db: db, ttl: ttl, lc: lc}, nil
}

// Add - Adds key to cache.
func (cache *Cache) Add(key string) (err error) {
	body, err := time.Now().MarshalBinary()
	if err != nil {
		return
	}
	cache.lc.Set(key, true)
	return cache.db.Put([]byte(key), body, nil)
}

// Has - Checks if cache has key.
func (cache *Cache) Has(key string) (_ bool, err error) {
	if _, has := cache.lc.Get(key); has {
		return true, nil
	}
	value, err := cache.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return
	}
	t := &time.Time{}
	t.UnmarshalBinary(value)
	if time.Since(*t) > cache.ttl {
		// Ignore deletion error just log it
		if e := cache.db.Delete([]byte(key), nil); e != nil {
			glog.Errorf("Cache delete error: %v", e)
		}
		return false, nil
	}
	return true, nil
}

// Close - Closes the LevelDB database.
func (cache *Cache) Close() error {
	return cache.db.Close()
}
