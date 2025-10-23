package lock

import (
	"context"
    "sync"

	api "dfs/proto-gen/lock"

	seelog "github.com/cihub/seelog"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type LockInfo struct {
	lockId string
	owner  string
	seqNum int64
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
	lm.lockManager[lockInfo.lockId] = lockInfo
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

// GRPCServer
type LockServiceServer struct {
	mu     sync.Mutex
	cond   *sync.Cond
	locked *LockManager
	grpc   *grpc.Server

	logger seelog.LoggerInterface

	api.UnimplementedLockServiceServer
}

func NewLockServer(grpcServer *grpc.Server, logger seelog.LoggerInterface) *LockServiceServer {
	s := &LockServiceServer{
		grpc:   grpcServer,
		logger: logger,
	}
	s.locked = NewLockManager()
	s.cond = sync.NewCond(&s.mu)

    s.logger.Infof("LockServiceServer initialized successfully")

	return s
}

func (s *LockServiceServer) Acquire(ctx context.Context, req *api.AcquireRequest) (*api.AcquireResponse, error) {
	s.mu.Lock()         // block server so there won`t be 2 person trying to get 1 file
	defer s.mu.Unlock() // auto unlock when function ends

    s.logger.Infof("received acquire request: lock_id=%s owner_id=%s seq=%d", req.LockId, req.OwnerId, req.Sequence)

	for s.locked.IsLocked(req.LockId) {
        s.logger.Infof("lock %s is currently held by another owner, waiting...", req.LockId)
		s.cond.Wait()
        // return &api.AcquireResponse{Success: proto.Bool(false)}, nil
	}

	s.locked.AddNewLockInfo(&LockInfo{
		lockId: req.LockId,
		owner:  req.OwnerId,
		seqNum: req.Sequence,
	})

    s.logger.Infof("Lock %s successfully acquired by owner %s", req.LockId, req.OwnerId)

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
	if req.OwnerId != lockInfo.owner {
        s.logger.Warnf("release denied for lock %s: owner mismatch (expected=%s, got=%s)",
			req.LockId, lockInfo.owner, req.OwnerId)

		return &api.ReleaseResponse{}, nil
	}

	if req.Sequence != lockInfo.seqNum {
        s.logger.Warnf("release denied for lock %s: sequence mismatch (expected=%d, got=%d)",
			req.LockId, lockInfo.seqNum, req.Sequence)

		return &api.ReleaseResponse{}, nil
	}

	s.locked.RemoveLock(req.LockId)
	s.cond.Broadcast()

    s.logger.Infof("lock %s released successfully by owner %s", req.LockId, req.OwnerId)

	return &api.ReleaseResponse{}, nil
}

func (s *LockServiceServer) Stop(ctx context.Context, req *api.StopRequest) (*api.StopResponse, error) {
    s.logger.Infof("received stop request — starting graceful shutdown")

	go func() {
		s.grpc.GracefulStop()
	}()

    s.logger.Infof("gRPC LockServer stopped successfully")

	return &api.StopResponse{}, nil
}
