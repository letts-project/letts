package handlers_test

import (
	"sync"
	"testing"
	"time"

	"letts/internal/server/handlers"
)

// TestKeyMutexExclusion verifies that two goroutines acquiring the same key
// are serialized: the second blocks until the first releases.
func TestKeyMutexExclusion(t *testing.T) {
	km := handlers.NewKeyMutex()

	var order []int
	var mu sync.Mutex
	record := func(n int) {
		mu.Lock()
		order = append(order, n)
		mu.Unlock()
	}

	unlock1 := km.Lock("key1")
	// Goroutine 2 tries to acquire same key — must block.
	var wg sync.WaitGroup
	wg.Add(1)
	started := make(chan struct{})
	go func() {
		defer wg.Done()
		close(started)
		unlock2 := km.Lock("key1")
		record(2)
		unlock2()
	}()

	// Wait until goroutine 2 is running (blocked on inner lock).
	<-started
	time.Sleep(5 * time.Millisecond) // let g2 reach the inner lock

	record(1)
	unlock1()

	wg.Wait()

	mu.Lock()
	got := order
	mu.Unlock()

	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Errorf("ordering: want [1 2], got %v", got)
	}
}

// TestKeyMutexDifferentKeys verifies that different keys do not block each other.
func TestKeyMutexDifferentKeys(t *testing.T) {
	km := handlers.NewKeyMutex()

	unlock1 := km.Lock("alpha")
	unlock2 := km.Lock("beta") // must not block

	if km.Size() != 2 {
		t.Errorf("size: want 2, got %d", km.Size())
	}

	unlock1()
	unlock2()

	if km.Size() != 0 {
		t.Errorf("size after release: want 0, got %d", km.Size())
	}
}

// TestKeyMutexNoLeak verifies that entries are removed once all holders release.
func TestKeyMutexNoLeak(t *testing.T) {
	km := handlers.NewKeyMutex()

	const n = 10
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := km.Lock("shared")
			defer unlock()
		}()
	}
	wg.Wait()

	if got := km.Size(); got != 0 {
		t.Errorf("entry leaked: Size()=%d, want 0", got)
	}
}
