package natsfs

import (
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// cacheEntry represents a cached object in the NATS filesystem with thread-safe access.
type cacheEntry struct {
	info      *nats.ObjectInfo // Stores NATS object metadata
	data      []byte           // Actual file data
	modTime   time.Time        // Last modification time
	expiry    time.Time        // When this cache entry expires
	accessing sync.RWMutex     // Mutex for thread-safe access
}

// newCacheEntry creates a new cache entry with the provided object info, data and TTL duration.
// The entry will expire after the specified TTL duration from the current time.
func newCacheEntry(info *nats.ObjectInfo, data []byte, ttl time.Duration) *cacheEntry {
	return &cacheEntry{
		info:    info,
		data:    data,
		modTime: info.ModTime,
		expiry:  time.Now().Add(ttl),
	}
}

// There isExpired checks if the cache entry has expired based on its expiry time.
// Returns true if the current time is after the entry's expiry time.
func (ce *cacheEntry) isExpired() bool {
	return time.Now().After(ce.expiry)
}

// getData retrieves the cached data in a thread-safe manner.
// Returns an error if the cache entry has expired, otherwise returns the cached data.
func (ce *cacheEntry) getData() ([]byte, error) {
	ce.accessing.RLock()
	defer ce.accessing.RUnlock()

	if ce.isExpired() {
		return nil, fmt.Errorf("cache entry expired")
	}
	return ce.data, nil
}

// getInfo retrieves the cached NATS object info in a thread-safe manner.
// Returns an error if the cache entry has expired, otherwise returns the cached object info.
func (ce *cacheEntry) getInfo() (*nats.ObjectInfo, error) {
	ce.accessing.RLock()
	defer ce.accessing.RUnlock()

	if ce.isExpired() {
		return nil, fmt.Errorf("cache entry expired")
	}
	return ce.info, nil
}
