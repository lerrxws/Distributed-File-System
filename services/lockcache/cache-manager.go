package dfs

import (
	"context"
	"fmt"
	"sync"

	lockapi "dfs/proto-gen/lock"

	seelog "github.com/cihub/seelog"
	"google.golang.org/protobuf/proto"
)

type CacheManager struct {
	cacheManager map[string]*CacheInfo
	lockClient   lockapi.LockServiceClient
	logger       seelog.LoggerInterface

	mu sync.Mutex
}

func NewCacheManager(lockClient lockapi.LockServiceClient, logger seelog.LoggerInterface) *CacheManager {
	return &CacheManager{
		cacheManager: make(map[string]*CacheInfo),
		lockClient:   lockClient,
		logger:       logger,
	}
}

func (cm *CacheManager) AddCache(newCache *CacheInfo) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.cacheManager[newCache.LockId] = newCache
}

func (cm *CacheManager) RemoveCache(lockId string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.cacheManager, lockId)
}

func (cm *CacheManager) GetCacheInfo(lockId string) *CacheInfo {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.cacheManager[lockId]
}

func (cm *CacheManager) IsCached(lockId string) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	_, exist := cm.cacheManager[lockId]
	return exist
}

func (cm *CacheManager) getOrCreateCache(lockId, ownerId string, seqNum int64) *CacheInfo {
	if !cm.IsCached(lockId) {
		cm.AddCache(NewCacheInfo(lockId))
	}

	cache := cm.GetCacheInfo(lockId)
	cache.SeqNum = seqNum
	cache.OwnerId = ownerId

	return cache
}

func (cm *CacheManager) Acquire(ctx context.Context, req *lockapi.AcquireRequest) (*lockapi.AcquireResponse, error) {
	cacheInfo := cm.getOrCreateCache(req.LockId, req.OwnerId, req.Sequence)

	for {
		select {
		case <-ctx.Done():
			cm.logger.Warnf("[CacheManager] Acquire for %s canceled by context", req.LockId)
			return &lockapi.AcquireResponse{Success: proto.Bool(false)}, ctx.Err()
		default:
		}

		cacheInfo.mu.Lock()
		state := cacheInfo.State
		cacheInfo.mu.Unlock()

		switch state {

		case None:
			cacheInfo.mu.Lock()
			cacheInfo.State = Acquiring
			cacheInfo.OwnerId = req.OwnerId
			cacheInfo.SeqNum = req.Sequence
			cacheInfo.mu.Unlock()

			cm.logger.Infof("[CacheManager] Trying to acquire lock %s (seq=%d)", req.LockId, req.Sequence)
			acqResp, err := cm.lockClient.Acquire(ctx, req)
			if err != nil {
				cm.logger.Errorf("[CacheManager] Acquire RPC failed for %s: %v", req.LockId, err)
				cacheInfo.mu.Lock()
				cacheInfo.State = None
				cacheInfo.mu.Unlock()
				return &lockapi.AcquireResponse{Success: proto.Bool(false)}, err
			}

			if *acqResp.Success {
				cacheInfo.mu.Lock()
				cacheInfo.State = Locked
				cacheInfo.mu.Unlock()
				cm.logger.Infof("[CacheManager] Lock %s acquired (seq=%d)", req.LockId, req.Sequence)
				return acqResp, nil
			}

			cacheInfo.mu.Lock()
			cm.logger.Infof("[CacheManager] Lock %s busy — waiting for Retry...", req.LockId)
			for cacheInfo.State == Acquiring {
				cacheInfo.cond.Wait()
			}
			cacheInfo.mu.Unlock()

		case Free:
			cacheInfo.mu.Lock()
			cacheInfo.State = Locked
			cacheInfo.OwnerId = req.OwnerId
			cacheInfo.SeqNum = req.Sequence
			cacheInfo.mu.Unlock()
			cm.logger.Infof("[CacheManager] Reused cached lock %s (local reuse)", req.LockId)
			return &lockapi.AcquireResponse{Success: proto.Bool(true)}, nil

		case Locked:
			cm.logger.Infof("[CacheManager] Lock %s already held locally", req.LockId)
			return &lockapi.AcquireResponse{Success: proto.Bool(true)}, nil

		case Acquiring:
			cm.logger.Infof("[CacheManager] Waiting for Retry for lock %s...", req.LockId)
			cacheInfo.mu.Lock()
			for cacheInfo.State == Acquiring {
				cacheInfo.cond.Wait()
			}
			cacheInfo.mu.Unlock()

		default:
			cm.logger.Warnf("[CacheManager] Unknown state %s for lock %s — resetting", state, req.LockId)
			cacheInfo.mu.Lock()
			cacheInfo.State = None
			cacheInfo.mu.Unlock()
		}
	}
}

func (cm *CacheManager) Release(ctx context.Context, req *lockapi.ReleaseRequest) (*lockapi.ReleaseResponse, error) {
	cacheInfo := cm.GetCacheInfo(req.LockId)
	if cacheInfo == nil {
		cm.logger.Errorf("[CacheManager] release: lock %s not found in cache", req.LockId)
		return &lockapi.ReleaseResponse{}, fmt.Errorf("release: lock %s not found in cache", req.LockId)

	}

	cacheInfo.mu.Lock()
	defer cacheInfo.mu.Unlock()

	switch cacheInfo.State {
	case Locked:
		cacheInfo.State = Free
		cacheInfo.cond.Broadcast()
		cm.logger.Infof("[CacheManager] Released lock %s → Free", req.LockId)
		return &lockapi.ReleaseResponse{}, nil

	case Free:
		cm.logger.Infof("[CacheManager] Lock %s already free", req.LockId)
		return &lockapi.ReleaseResponse{}, nil

	default:
		return nil, fmt.Errorf("[CacheManager] release: invalid state for lock %s (%s)", req.LockId, cacheInfo.State)
	}
}

func (cm *CacheManager) ReleaseRPC(ctx context.Context, req *lockapi.ReleaseRequest) (*lockapi.ReleaseResponse, error) {
	cacheInfo := cm.GetCacheInfo(req.LockId)

	resp, err := cm.lockClient.Release(ctx, req)
	if err != nil {
		cm.logger.Errorf("[CacheManager] Release RPC failed for %s: %v", req.LockId, err)
		return nil, err
	}

	cacheInfo.mu.Lock()
	cacheInfo.State = None
	cacheInfo.cond.Broadcast()
	cacheInfo.mu.Unlock()

	cm.logger.Infof("[CacheManager] Release RPC succeeded for %s → None", req.LockId)
	return resp, nil
}
