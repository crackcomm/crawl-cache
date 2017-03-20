package cache

// Cache - Crawl cache interface.
type Cache interface {
	// Add - Adds key to cache.
	Add(string) error
	// Has - Checks if cache has key.
	Has(string) (bool, error)
}
