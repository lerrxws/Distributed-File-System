package dfs

import (
	"context"
	lcapi "dfs/proto-gen/lockcache"

	seelog "github.com/cihub/seelog"
	"google.golang.org/grpc"
)

type LockCacheService struct {
	grpc         *grpc.Server
	cacheManager *CacheManager
	releaser     *ReleaserTask
	logger       seelog.LoggerInterface

	lcapi.UnimplementedLockCacheServiceServer
}

func NewLockCacheService(grpcServer *grpc.Server, cacheManager *CacheManager, releaser *ReleaserTask, logger seelog.LoggerInterface) *LockCacheService {
	logger.Infof("[LockCacheService] Initializing LockCache service...")

	srv := &LockCacheService{
		grpc:         grpcServer,
		cacheManager: cacheManager,
		releaser:     releaser,
		logger:       logger,
	}

	logger.Infof("[LockCacheService] Successfully initialized (attached to gRPC server).")
	return srv
}

func (cl *LockCacheService) Revoke(ctx context.Context, req *lcapi.RevokeRequest) (*lcapi.RevokeResponse, error) {
	lockId := req.LockId
	cl.logger.Infof("[LockCacheService] Received revoke request for lock %s", lockId)

	if !cl.cacheManager.IsCached(lockId) {
		cl.logger.Warnf("[LockCacheService] Revoke requested for unknown lock %s — ignoring", lockId)
		return &lcapi.RevokeResponse{}, nil
	}

	cacheInfo := cl.cacheManager.GetCacheInfo(lockId)

	cacheInfo.mu.Lock()
	defer cacheInfo.mu.Unlock()

	switch cacheInfo.State {
	case None:
		cl.logger.Infof("[LockCacheService] Lock %s already None — no action", cacheInfo.LockId)
		return &lcapi.RevokeResponse{}, nil

	case Locked:
		cl.logger.Infof("[LockCacheService] Lock %s is Locked — waiting until it becomes Free...", cacheInfo.LockId)
		for cacheInfo.State == Locked {
			cacheInfo.cond.Wait()
		}
		// після виходу зі стану Locked — він або Free, або None
		fallthrough

	case Free:
		cl.logger.Infof("[LockCacheService] Lock %s now Free — sending to Releaser", cacheInfo.LockId)
		cl.releaser.AddTask(cacheInfo)
		return &lcapi.RevokeResponse{}, nil

	default:
		cl.logger.Warnf("[LockCacheService] Unexpected lock state %s for %s — skipping revoke", cacheInfo.State, cacheInfo.LockId)
		return &lcapi.RevokeResponse{}, nil
	}
}

func (cl *LockCacheService) Retry(ctx context.Context, req *lcapi.RetryRequest) (*lcapi.RetryResponse, error) {
	return &lcapi.RetryResponse{}, nil
}

func (cl *LockCacheService) Stop() {
	cl.logger.Infof("[LockCacheService] received stop request — starting graceful shutdown")

	cl.releaser.Stop()

	cl.logger.Infof("[LockCacheService] gRPC LockServer stopped successfully")
}
