package dfs

import (
	"context"
	"log"
	"time"

	cache "dfs/services/dfs/lockcache"
	lockapi "dfs/proto-gen/lock"
)

type Releaser struct {
	cache      *cache.LockCache
	lockClient lockapi.LockServiceClient
}

func newReleaser(cache *cache.LockCache, lockClient lockapi.LockServiceClient) *Releaser {
	return &Releaser{
		cache:      cache,
		lockClient: lockClient,
	}
}

func (r *Releaser) StartReleaser() {
	go func() {
		for lockId := range r.cache.RevokeQueue {
			log.Printf("[Releaser] got revoke for %s", lockId)
			r.handleQueuedRevoke(lockId)
		}
	}()
}

func (r *Releaser) handleQueuedRevoke(lockId string) {
	for {
		r.cache.Mu.Lock()
		lock := r.cache.GetLock(lockId)
		r.cache.Mu.Unlock()

		if lock == nil {
			log.Printf("[Releaser] lock %s does not exist", lockId)
			return
		}

		if lock.State == cache.Free {
			log.Printf("[Releaser] lock %s is free, releasing to server...", lockId)

			r.sendReleaseRPC(lockId)

			r.cache.Mu.Lock()
			r.cache.SetLockState(lock, cache.None)
			lock.Cond.Broadcast()
			r.cache.Mu.Unlock()

			log.Printf("[Releaser] lock %s released and set to None", lockId)
			break
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (r *Releaser) sendReleaseRPC(lockId string) {
	log.Printf("[Releaser] Sending release() for lock [%s] to LockService.", lockId)

	// TODO: rewrite user to format ip:port:ownerid
	_, err := r.lockClient.Release(context.Background(), &lockapi.ReleaseRequest{
		LockId:  lockId,
		OwnerId: "user",
	})
	if err != nil {
		log.Printf("[Releaser] Release RPC for lock [%s] failed: %v", lockId, err)
	} else {
		log.Printf("[Releaser] Release RPC for lock [%s] completed successfully", lockId)
	}
}