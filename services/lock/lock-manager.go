package lock

import (
	"sync"
)

type LockInfo struct {
	LockId string
	Owner  string
	SeqNum int64

	Mu   sync.Mutex
	Cond *sync.Cond
}

type LockManager struct {
	lockManager map[string]*LockInfo
}

func NewLockManager() *LockManager {
	return &LockManager{
		lockManager: make(map[string]*LockInfo),
	}
}

func (lm *LockManager) AddNewLockInfo(lockInfo *LockInfo) {
	lm.lockManager[lockInfo.LockId] = lockInfo
}

func (lm *LockManager) RemoveLock(lockId string) {
	delete(lm.lockManager, lockId)
}

func (lm *LockManager) IsLocked(lockId string) bool {
	_, isLocked := lm.lockManager[lockId]
	return isLocked
}

func (lm *LockManager) GetLockInfo(lockId string) *LockInfo {
	return lm.lockManager[lockId]
}