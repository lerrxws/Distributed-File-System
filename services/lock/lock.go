package lock

import (
	"context"
	"sync"

	api "dfs/proto-gen/lock"

	seelog "github.com/cihub/seelog"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// GRPCServer
type LockServiceServer struct {
	mu     sync.Mutex
	cond   *sync.Cond
	locked *LockManager
	grpc   *grpc.Server

	revoker *RevokeTask
	retrier *RetryTask

	logger seelog.LoggerInterface

	api.UnimplementedLockServiceServer
}

func NewLockServer(grpcServer *grpc.Server, revoker *RevokeTask, retrier *RetryTask, logger seelog.LoggerInterface) *LockServiceServer {
	s := &LockServiceServer{
		grpc:   grpcServer,
		revoker: revoker,
		retrier: retrier,
		logger: logger,
	}

	s.revoker.Start()
	s.retrier.Start()

	s.locked = NewLockManager()
	s.cond = sync.NewCond(&s.mu)

	s.logger.Infof("LockService] LockServiceServer initialized successfully")

	return s
}

func (s *LockServiceServer) Acquire(ctx context.Context, req *api.AcquireRequest) (*api.AcquireResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Infof("[Acquire] received acquire request: lock_id=%s owner_id=%s seq=%d", req.LockId, req.OwnerId, req.Sequence)

	for s.locked.IsLocked(req.LockId) {
		s.logger.Infof(
			"[Acquire] acquire denied: lock_id=%s already held by another client",
			req.LockId,
		)

		lockInfo := s.locked.GetLockInfo(req.LockId)

		s.revoker.AddTask(&RevokerTarget{
			LockId:  lockInfo.LockId,
			OwnerId: lockInfo.Owner,
		})
		s.retrier.AddClientForRetry(req.OwnerId, req.LockId, req.Sequence)

		return &api.AcquireResponse{Success: proto.Bool(false)}, nil
	}

	s.locked.AddNewLockInfo(&LockInfo{
		LockId: req.LockId,
		Owner:  req.OwnerId,
		SeqNum: req.Sequence,
	})

	s.logger.Infof("[Acquire] lock %s successfully acquired by owner %s", req.LockId, req.OwnerId)

	return &api.AcquireResponse{Success: proto.Bool(true)}, nil
}

func (s *LockServiceServer) Release(ctx context.Context, req *api.ReleaseRequest) (*api.ReleaseResponse, error) {
	s.logger.Infof("[Release] Release request received: lock_id=%s owner_id=%s seq=%d",
		req.LockId, req.OwnerId, req.Sequence)

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.locked.IsLocked(req.LockId) {
		s.logger.Warnf("[Release] Lock %s already released or not found", req.LockId)
		return &api.ReleaseResponse{}, nil
	}

	lockInfo := s.locked.GetLockInfo(req.LockId)
	if req.OwnerId != lockInfo.Owner {
		s.logger.Warnf("[Release] Denied for lock %s: owner mismatch (expected=%s, got=%s)",
			req.LockId, lockInfo.Owner, req.OwnerId)
		return &api.ReleaseResponse{}, nil
	}

	// if req.Sequence != lockInfo.SeqNum {
	// 	s.logger.Warnf("[Release] Denied for lock %s: sequence mismatch (expected=%d, got=%d)",
	// 		req.LockId, lockInfo.SeqNum, req.Sequence)
	// 	return &api.ReleaseResponse{}, nil
	// }

	s.retrier.EnqueueRetriesForLock(req.LockId)
	s.locked.RemoveLock(req.LockId)
	s.logger.Infof("[Release] Lock %s released successfully by owner %s", req.LockId, req.OwnerId)

	return &api.ReleaseResponse{}, nil
}

func (s *LockServiceServer) Stop(ctx context.Context, req *api.StopRequest) (*api.StopResponse, error) {
    s.logger.Infof("[Stop] Received stop request — initiating graceful shutdown...")

    s.revoker.Stop()
    s.retrier.Stop()

    go func() {
        s.grpc.GracefulStop()
        s.logger.Infof("[Stop] gRPC LockServer stopped successfully")
    }()

    return &api.StopResponse{}, nil
}
