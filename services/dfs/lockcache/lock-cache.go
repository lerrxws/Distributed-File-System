package lockcache

import (
	"log"
	"sync"
)

type LockCache struct {
	Mu    sync.Mutex
	locks map[string]*Lock
	RevokeQueue chan string
}

func NewLockCache() *LockCache {
	return &LockCache{
		locks: make(map[string]*Lock),
		RevokeQueue: make(chan string, 1000),
	}
}

func (lc *LockCache) GetLock(resourceID string) (*Lock) {
	lc.Mu.Lock()
	defer lc.Mu.Unlock()

	lock, exists := lc.locks[resourceID]
	if !exists {
		return nil
	}

	return lock
}

func (lc *LockCache) AddLock(resourceID string, state LockState) *Lock {
	lc.Mu.Lock()
	defer lc.Mu.Unlock()

	lock := NewLock(state)
	lc.locks[resourceID] = lock
	return lock
}

func (lc *LockCache) SetLockState(lock *Lock, state LockState) bool {
	lc.Mu.Lock()
	defer lc.Mu.Unlock()

	if lock == nil {
		log.Printf("SetLockState called with nil lock")
		return false
	}

	lock.State = state
	// lock.Cond.Broadcast() // wake up any waiting goroutines
	return true
}