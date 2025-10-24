package dfs

import (
	"sync"
)

type CacheInfo struct {
	LockId  string
	OwnerId string
	SeqNum  int64
	State   CacheState

	mu   sync.Mutex
	cond *sync.Cond
}

func NewCacheInfo(lockId string) *CacheInfo {
	ci := &CacheInfo{
		LockId: lockId,
		State:  None,
	}
	ci.cond = sync.NewCond(&ci.mu)
	return ci
}

func (ci *CacheInfo) SetState(state CacheState) {
	ci.State = state
}