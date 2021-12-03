package index

import (
	"sync"
)

type HashIndexer struct {
	m sync.Map
}

func NewHashIndexer() *HashIndexer {
	return &HashIndexer{
		m: sync.Map{},
	}
}

// implement indexer

// Write stores the key into indexer.
func (h *HashIndexer) Write(key string, inode INode) {
	h.m.Store(key, inode)
}

// Read reads the index information by given key.
func (h *HashIndexer) Read(key string) (INode, bool) {
	var ok bool
	val, ok := h.m.Load(key)
	if !ok {
		return INode{}, false
	}
	val2, ok := val.(INode)
	return val2, ok
}

// Read reads the index information by given key.
func (h *HashIndexer) Delete(key string) {
	h.m.Delete(key)
}
