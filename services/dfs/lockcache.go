package dfs

import (
	"context"
	"log"

	api "dfs/proto-gen/lockcache"
	cache "dfs/services/dfs/lockcache"
)

type LockCacheService struct {
	lockCache *cache.LockCache

	api.UnimplementedLockCacheServiceServer
}

func NewLockCacheService(lockCache *cache.LockCache) *LockCacheService {
	return &LockCacheService{
		lockCache: lockCache,
	}
}

func (s *LockCacheService) Revoke(ctx context.Context, req *api.RevokeRequest) (*api.RevokeResponse, error) {
	lockId := req.LockId

	s.lockCache.Mu.Lock()
	lock := s.lockCache.GetLock(lockId)
	s.lockCache.Mu.Unlock()

	if lock == nil {
		log.Printf("[LockCacheService] Revoke(%s): lock not found\n", lockId)
		return &api.RevokeResponse{}, nil
	}

	switch lock.State {
	case cache.Locked:
		log.Printf("[LockCacheService] Revoke(%s): lock is Locked — queued for release\n", lockId)
		s.lockCache.RevokeQueue <- lockId

	case cache.Free:
		log.Printf("[LockCacheService] Revoke(%s): lock is Free — queued for release\n", lockId)
		s.lockCache.RevokeQueue <- lockId

	default:
		log.Printf("[LockCacheService] Revoke(%s): ignored, state=%v\n", lockId, lock.State)
	}

	return &api.RevokeResponse{}, nil
}

func (s *LockCacheService) Retry( ctx context.Context, req *api.RetryRequest) (*api.RetryResponse, error) {
	s.lockCache.Mu.Lock()
	defer s.lockCache.Mu.Unlock()

	lock := s.lockCache.GetLock(req.LockId)
	if lock == nil {
		log.Printf("[LockCacheService] Retry(%s): lock not found\n", req.LockId)
		return &api.RetryResponse{}, nil
	}

	if lock.State == cache.Acquiring {
		log.Printf("[LockCacheService] Retry(%s): waking up waiting goroutines\n", req.LockId)
		lock.Cond.Broadcast()
	} else {
		log.Printf("[LockCacheService] Retry(%s): ignored, state=%v\n", req.LockId, lock.State)
	}

	return &api.RetryResponse{}, nil
}