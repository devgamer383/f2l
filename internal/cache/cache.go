package cache

import (
	"container/list"
	"sync"
	"time"
)

type cacheItem struct {
	key       string
	data      []byte
	size      int64
	expiresAt time.Time
	element   *list.Element
}

type LRUCache struct {
	mu        sync.RWMutex
	items     map[string]*cacheItem
	evictList *list.List
	maxBytes  int64
	currBytes int64
	ttl       time.Duration

	hits   int64
	misses int64
}

func NewLRUCache(maxMB int, ttlSec int) *LRUCache {
	if maxMB < 16 {
		maxMB = 64
	}
	if ttlSec < 60 {
		ttlSec = 1800
	}

	c := &LRUCache{
		items:     make(map[string]*cacheItem),
		evictList: list.New(),
		maxBytes:  int64(maxMB) * 1024 * 1024,
		ttl:       time.Duration(ttlSec) * time.Second,
	}

	// Periodic background cleanup of expired entries
	go c.cleanupLoop()

	return c
}

func (c *LRUCache) cleanupLoop() {
	ticker := time.NewTicker(2 * time.Minute)
	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for key, item := range c.items {
			if now.After(item.expiresAt) {
				c.removeElement(item)
				delete(c.items, key)
			}
		}
		c.mu.Unlock()
	}
}

func (c *LRUCache) removeElement(item *cacheItem) {
	c.evictList.Remove(item.element)
	c.currBytes -= item.size
}

func (c *LRUCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, found := c.items[key]
	if !found {
		c.misses++
		return nil, false
	}

	if time.Now().After(item.expiresAt) {
		c.removeElement(item)
		delete(c.items, key)
		c.misses++
		return nil, false
	}

	c.evictList.MoveToFront(item.element)
	c.hits++
	return item.data, true
}

func (c *LRUCache) Set(key string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	dataSize := int64(len(data))
	if dataSize > c.maxBytes {
		return // Don't cache chunks larger than the entire cache
	}

	now := time.Now()

	// If key exists, update it
	if item, found := c.items[key]; found {
		c.currBytes -= item.size
		item.data = data
		item.size = dataSize
		item.expiresAt = now.Add(c.ttl)
		c.currBytes += dataSize
		c.evictList.MoveToFront(item.element)
	} else {
		// Evict oldest until space is available
		for c.currBytes+dataSize > c.maxBytes && c.evictList.Len() > 0 {
			oldest := c.evictList.Back()
			if oldest != nil {
				oldItem := oldest.Value.(*cacheItem)
				delete(c.items, oldItem.key)
				c.removeElement(oldItem)
			}
		}

		item := &cacheItem{
			key:       key,
			data:      data,
			size:      dataSize,
			expiresAt: now.Add(c.ttl),
		}
		item.element = c.evictList.PushFront(item)
		c.items[key] = item
		c.currBytes += dataSize
	}
}

type Stats struct {
	CachedChunks int     `json:"cached_chunks"`
	CachedSizeMB float64 `json:"cached_size_mb"`
	MaxSizeMB    float64 `json:"max_size_mb"`
	Hits         int64   `json:"hits"`
	Misses       int64   `json:"misses"`
	HitRatio     float64 `json:"hit_ratio_percent"`
}

func (c *LRUCache) GetStats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	totalReqs := c.hits + c.misses
	hitRatio := 0.0
	if totalReqs > 0 {
		hitRatio = (float64(c.hits) / float64(totalReqs)) * 100.0
	}

	return Stats{
		CachedChunks: len(c.items),
		CachedSizeMB: float64(c.currBytes) / (1024 * 1024),
		MaxSizeMB:    float64(c.maxBytes) / (1024 * 1024),
		Hits:         c.hits,
		Misses:       c.misses,
		HitRatio:     hitRatio,
	}
}
