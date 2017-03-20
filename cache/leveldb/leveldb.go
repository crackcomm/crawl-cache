package leveldb

import "github.com/syndtr/goleveldb/leveldb"

// Cache - LevelDB cache.
type Cache struct {
	db *leveldb.DB
}

// Open - Opens LevelDB cache.
func Open(path string) (cache *Cache, err error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return
	}
	return &Cache{db: db}, nil
}

// Add - Adds key to cache.
func (cache *Cache) Add(key string) (err error) {
	return cache.db.Put([]byte(key), nil, nil)
}

// Has - Checks if cache has key.
func (cache *Cache) Has(key string) (_ bool, err error) {
	_, err = cache.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return
	}
	return true, nil
}

// Close - Closes the LevelDB database.
func (cache *Cache) Close() error {
	return cache.db.Close()
}
