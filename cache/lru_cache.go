package cache

import "sync"

type lruNode struct {
	key   string
	value interface{}
	next  *lruNode
	prev  *lruNode
}

type lruCache struct {
	size     int
	capacity int
	head     *lruNode
	tail     *lruNode
	hm       map[string]*lruNode
	mu       sync.RWMutex
}

func NewLRUCache(cap int) *lruCache {
	dummy := lruNode{}
	return &lruCache{
		capacity: cap,
		head:     &dummy,
		tail:     &dummy,
		hm:       map[string]*lruNode{},
		mu:       sync.RWMutex{},
	}
}

// delete removes the lrunode from double linked list
func (node *lruNode) delete() {
	p := node.prev
	p.next = node.next
	if node.next != nil {
		node.next.prev = p
	}
	node.next = nil
	node.prev = nil
}

// insert the node after given node.
func (node *lruNode) insertAfter(after *lruNode) {
	n := after.next
	if n != nil {
		n.prev = node
	}
	after.next = node
	node.prev = after
	node.next = n

}

// == implement cacher

func (lru *lruCache) Load(key string) (interface{}, bool) {

	lru.mu.RLock()
	node, ok := lru.hm[key]
	if !ok {
		lru.mu.RUnlock()
		return nil, false
	}
	lru.mu.RUnlock()

	// found, need to put the node to the leftmost pos
	lru.mu.Lock()
	defer lru.mu.Unlock()
	if node == lru.tail {
		lru.tail = node.prev
	}
	node.delete()
	node.insertAfter(lru.head)
	return node.value, true
}

func (lru *lruCache) Store(key string, value interface{}) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	// if key exist, just update its value
	node, ok := lru.hm[key]
	if ok {
		node.value = value
		return
	}

	// otherwise, create a new node
	node = &lruNode{
		key:   key,
		value: value,
	}
	lru.hm[key] = node

	if lru.capacity == lru.size {
		// need to evict
		delete(lru.hm, lru.tail.key)
		p := lru.tail.prev
		lru.tail.delete()
		lru.tail = p
	} else {
		lru.size += 1
	}
	// normal append
	t := lru.tail
	node.prev = t
	t.next = node
	lru.tail = node
}

func (lru *lruCache) Delete(key string) {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	node, ok := lru.hm[key]
	if !ok {
		return
	}
	// do delete
	delete(lru.hm, key)
	if node == lru.tail {
		lru.tail = lru.tail.prev
	}
	node.delete()
}
