package kafka_client

import (
	"container/list"
	"os"
	"strconv"
	"sync"
)

type lruCacheEntry[T any] struct {
	key   string
	value T
}

type lruCache[T any] struct {
	mu         sync.Mutex
	maxEntries int
	items      map[string]*list.Element
	order      *list.List
}

func newLRUCache[T any](maxEntries int) *lruCache[T] {
	if maxEntries < 1 {
		maxEntries = 1
	}
	return &lruCache[T]{
		maxEntries: maxEntries,
		items:      make(map[string]*list.Element, maxEntries),
		order:      list.New(),
	}
}

func (c *lruCache[T]) Get(key string) (T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		c.order.MoveToFront(elem)
		return elem.Value.(*lruCacheEntry[T]).value, true
	}
	var zero T
	return zero, false
}

func (c *lruCache[T]) Add(key string, value T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		elem.Value.(*lruCacheEntry[T]).value = value
		c.order.MoveToFront(elem)
		return
	}
	elem := c.order.PushFront(&lruCacheEntry[T]{key: key, value: value})
	c.items[key] = elem
	if c.order.Len() > c.maxEntries {
		c.removeOldest()
	}
}

func (c *lruCache[T]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

func (c *lruCache[T]) removeOldest() {
	tail := c.order.Back()
	if tail == nil {
		return
	}
	entry := tail.Value.(*lruCacheEntry[T])
	delete(c.items, entry.key)
	c.order.Remove(tail)
}

func cacheSizeFromEnv(envVar string, fallback int) int {
	if fallback < 1 {
		fallback = 1
	}
	raw, ok := os.LookupEnv(envVar)
	if !ok || raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return fallback
	}
	return n
}
