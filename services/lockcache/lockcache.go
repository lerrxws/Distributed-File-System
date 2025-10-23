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
	cl.logger.Infof("[Revoker] revoke request")
	cacheInfo := cl.cacheManager.GetCacheInfo(req.LockId)
	cl.releaser.AddTask(cacheInfo)
	return &lcapi.RevokeResponse{}, nil
}

func (cl *LockCacheService) Retry(ctx context.Context, req *lcapi.RetryRequest) (*lcapi.RetryResponse, error) {
	return &lcapi.RetryResponse{}, nil
}

func (cl *LockCacheService) Stop() {
	cl.logger.Infof("[LockCacheService] received stop request — starting graceful shutdown")

	cl.releaser.Stop()

	cl.logger.Infof("[LockCacheService] gRPC LockServer stopped successfully")
}
