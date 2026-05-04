package handlers

import "sync"

// KeyMutex provides per-key serialization. Entries are reference-counted and
// removed once unused.
type KeyMutex struct {
	mu sync.Mutex
	m  map[string]*kmEntry
}

type kmEntry struct {
	mu       sync.Mutex
	refCount int
}

// NewKeyMutex allocates a ready-to-use KeyMutex.
func NewKeyMutex() *KeyMutex {
	return &KeyMutex{m: make(map[string]*kmEntry)}
}

// Lock acquires the mutex for key and returns the unlock func. The caller must
// call the returned function exactly once to release the lock. Entries are
// automatically deleted from the map when their refcount reaches zero.
func (k *KeyMutex) Lock(key string) func() {
	k.mu.Lock()
	e, ok := k.m[key]
	if !ok {
		e = &kmEntry{}
		k.m[key] = e
	}
	e.refCount++
	k.mu.Unlock()

	e.mu.Lock()

	return func() {
		e.mu.Unlock()
		k.mu.Lock()
		e.refCount--
		if e.refCount == 0 {
			delete(k.m, key)
		}
		k.mu.Unlock()
	}
}

// Size returns the number of keys currently tracked. Intended for tests.
func (k *KeyMutex) Size() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.m)
}
