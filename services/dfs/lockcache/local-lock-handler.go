package lockcache

import (
	"context"
	"log"

	lockapi "dfs/proto-gen/lock"
)

type LocalLockHandler struct {
	lockCache  *LockCache
	lockClient lockapi.LockServiceClient
}

func NewLocalLockHandler(lockCache *LockCache, lockClient lockapi.LockServiceClient) *LocalLockHandler {
	return &LocalLockHandler{
		lockCache:  lockCache,
		lockClient: lockClient,
	}
}

// ця функція працює так що блокує запити і коли отрмимує бродкаст (при цьому статус лок змінюється на ноне) то вона пробує ще раз і виходить
func (lh *LocalLockHandler) AcquireLock(lockId string, userId string) bool {
	lh.lockCache.Mu.Lock()
	lock, exists := lh.lockCache.locks[lockId]
	if !exists {
		lock = lh.lockCache.AddLock(lockId, None)
	}
	lh.lockCache.Mu.Unlock()

	for {
		lh.lockCache.Mu.Lock()
		switch lock.State {

		case None:
			log.Printf("[Acquire] Lock %s is None → sending AcquireRPC", lockId)
			lh.lockCache.Mu.Unlock()

			resp, err := lh.lockClient.Acquire(context.Background(), &lockapi.AcquireRequest{
				LockId:  lockId,
				OwnerId: userId,
			})
			if err != nil {
				log.Printf("[Acquire] RPC failed for %s: %v", lockId, err)
				return false
			}

			lh.lockCache.Mu.Lock()
			if resp.Success != nil && *resp.Success {
				log.Printf("[Acquire] Acquired lock %s from server", lockId)
				lh.lockCache.SetLockState(lock, Locked)
				lh.lockCache.Mu.Unlock()
				return true
			} else {
				log.Printf("[Acquire] Lock %s busy → moving to Acquiring and waiting for Retry()", lockId)
				lh.lockCache.SetLockState(lock, Acquiring)
				lock.Cond.Wait()
			}
			lh.lockCache.Mu.Unlock()

		case Free:
			log.Printf("[Acquire] Lock %s is Free → acquiring locally", lockId)
			lh.lockCache.SetLockState(lock, Locked)
			lh.lockCache.Mu.Unlock()
			return true

		case Locked, Acquiring, Releasing:
			log.Printf("[Acquire] Lock %s in state %v → waiting...", lockId, lock.State)
			lock.Cond.Wait()
			lh.lockCache.Mu.Unlock()

		default:
			log.Printf("[Acquire] Unknown state for %s, aborting...", lockId)
			lh.lockCache.Mu.Unlock()
			return false
		}
	}
}

func (lh *LocalLockHandler) ReleaseLock(lockId string, userId string) {
	lh.lockCache.Mu.Lock()
	defer lh.lockCache.Mu.Unlock()

	lock, exists := lh.lockCache.locks[lockId]
	if !exists {
		log.Printf("Lock %s does not exist\n", lockId)
		return
	}

	if lock.State != Locked {
		log.Printf("Lock %s is not locked, cannot release\n", lockId)
		return
	}

	log.Printf("Releasing lock %s\n", lockId)
	lh.lockCache.SetLockState(lock, Free)
	lock.Cond.Broadcast()
}