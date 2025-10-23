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
	mu          sync.Mutex
	cond        *sync.Cond
	locked      *LockManager
	grpc        *grpc.Server

    revoker *RevokerTask

	logger seelog.LoggerInterface

	api.UnimplementedLockServiceServer
}

func NewLockServer(grpcServer *grpc.Server, logger seelog.LoggerInterface) *LockServiceServer {
	s := &LockServiceServer{
		grpc:   grpcServer,
		logger: logger,
	}
    s.revoker = NewRevokerTask(s.logger)
    s.revoker.Start()

	s.locked = NewLockManager()
	s.cond = sync.NewCond(&s.mu)

	s.logger.Infof("LockServiceServer initialized successfully")

	return s
}

func (s *LockServiceServer) Acquire(ctx context.Context, req *api.AcquireRequest) (*api.AcquireResponse, error) {
	s.mu.Lock()         // block server so there won`t be 2 person trying to get 1 file
	defer s.mu.Unlock() // auto unlock when function ends

	s.logger.Infof("[Acquire] received acquire request: lock_id=%s owner_id=%s seq=%d", req.LockId, req.OwnerId, req.Sequence)

	for s.locked.IsLocked(req.LockId) {
		s.logger.Infof(
            "[Acquire] acquire denied: lock_id=%s already held by another client",
            req.LockId,
        )

        lockInfo := s.locked.GetLockInfo(req.LockId)

        s.revoker.AddTask(&RevokerTarget{
            LockId: lockInfo.LockId,
            OwnerId: lockInfo.Owner,
        })

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
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Infof("release request received: lock_id=%s owner_id=%s seq=%d", req.LockId, req.OwnerId, req.Sequence)

	if !s.locked.IsLocked(req.LockId) {
		s.logger.Warnf("lock %s already released or not found", req.LockId)

		return &api.ReleaseResponse{}, nil
	}

	lockInfo := s.locked.GetLockInfo(req.LockId)
	if req.OwnerId != lockInfo.Owner {
		s.logger.Warnf("release denied for lock %s: owner mismatch (expected=%s, got=%s)",
			req.LockId, lockInfo.Owner, req.OwnerId)

		return &api.ReleaseResponse{}, nil
	}

	if req.Sequence != lockInfo.SeqNum {
		s.logger.Warnf("release denied for lock %s: sequence mismatch (expected=%d, got=%d)",
			req.LockId, lockInfo.SeqNum, req.Sequence)

		return &api.ReleaseResponse{}, nil
	}

	s.locked.RemoveLock(req.LockId)
	s.cond.Broadcast()

	s.logger.Infof("lock %s released successfully by owner %s", req.LockId, req.OwnerId)

	return &api.ReleaseResponse{}, nil
}

func (s *LockServiceServer) Stop(ctx context.Context, req *api.StopRequest) (*api.StopResponse, error) {
	s.logger.Infof("[LockService] received stop request — starting graceful shutdown")

	go func() {
		s.grpc.GracefulStop()
	}()

	s.logger.Infof("[LockService] gRPC LockServer stopped successfully")

	return &api.StopResponse{}, nil
}
