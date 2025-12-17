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

func (s *LockServiceServer) Acquire(
	ctx context.Context,
	req *api.AcquireRequest,
) (*api.AcquireResponse, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Infof(
		"[Acquire] Request received: lock_id=%s owner_id=%s seq=%d",
		req.LockId,
		req.OwnerId,
		req.Sequence,
	)

	if s.isDuplicateAcquire(req) {
		s.handleDuplicateAcquire(req)
		return s.acquireResponse(false)
	}

	if s.lockHeldByAnother(req) {
		s.handleContendedAcquire(req)
		return s.acquireResponse(false)
	}

	s.grantLock(req)
	return s.acquireResponse(true)
}

func (s *LockServiceServer) Release(
	ctx context.Context,
	req *api.ReleaseRequest,
) (*api.ReleaseResponse, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Infof(
		"[Release] Request received: lock_id=%s owner_id=%s seq=%d",
		req.LockId,
		req.OwnerId,
		req.Sequence,
	)

	if !s.lockExists(req) {
		return s.releaseResponse()
	}

	if !s.isLockedByReqOwner(req) {
		return s.releaseResponse()
	}

	if s.isDuplicateRelease(req) {
			s.logger.Infof(
			"[Acquire] Duplicate request ignored: owner=%s seq=%d",
			req.OwnerId,
			req.Sequence,
		)

		return s.releaseResponse()
	}

	s.releaseLock(req)
	return s.releaseResponse()
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

// region private methods

// region acquire

func (s *LockServiceServer) acquireResponse(success bool) (*api.AcquireResponse, error) {
	return &api.AcquireResponse{Success: proto.Bool(success)}, nil
}

func (s *LockServiceServer) isDuplicateAcquire(req *api.AcquireRequest) bool {
	return req.Sequence <= s.locked.GetBiggestSeqNumForUser(req.OwnerId)
}

func (s *LockServiceServer) lockHeldByAnother(req *api.AcquireRequest) bool {
	return s.locked.IsLocked(req.LockId) &&
		s.locked.GetOwnerOfLock(req.LockId) != req.OwnerId
}

func (s *LockServiceServer) handleDuplicateAcquire(req *api.AcquireRequest) {
	s.logger.Infof(
		"[Acquire] Duplicate request ignored: owner=%s seq=%d",
		req.OwnerId,
		req.Sequence,
	)

	if s.lockHeldByAnother(req) {
		lockInfo := s.locked.GetLockInfo(req.LockId)

		s.revoker.AddTask(&RevokerTarget{
			LockId:  lockInfo.LockId,
			OwnerId: lockInfo.Owner,
		})

		s.retrier.AddClientForRetry(req.OwnerId, req.LockId, req.Sequence)
	}
}

func (s *LockServiceServer) handleContendedAcquire(req *api.AcquireRequest) {
	lockInfo := s.locked.GetLockInfo(req.LockId)

	s.logger.Infof(
		"[Acquire] Lock %s held by another client, sending revoke",
		req.LockId,
	)

	s.revoker.AddTask(&RevokerTarget{
		LockId:  lockInfo.LockId,
		OwnerId: lockInfo.Owner,
	})

	s.retrier.AddClientForRetry(req.OwnerId, req.LockId, req.Sequence)
}

func (s *LockServiceServer) grantLock(req *api.AcquireRequest) {
	s.locked.AddNewLockInfo(&LockInfo{
		LockId: req.LockId,
		Owner:  req.OwnerId,
		SeqNum: req.Sequence,
	})

	s.logger.Infof(
		"[Acquire] Lock %s granted to owner %s",
		req.LockId,
		req.OwnerId,
	)
}

// endregion

// region release

func (s *LockServiceServer) releaseResponse() (*api.ReleaseResponse, error) {
	return &api.ReleaseResponse{}, nil
}

func (s *LockServiceServer) isDuplicateRelease(req *api.ReleaseRequest) bool {
	lockInfo := s.locked.GetLockInfo(req.LockId)
	return lockInfo != nil && req.OwnerId == lockInfo.Owner && req.Sequence < lockInfo.SeqNum
}

func (s *LockServiceServer) lockExists(req *api.ReleaseRequest) bool {
	if !s.locked.IsLocked(req.LockId) {
		s.logger.Warnf(
			"[Release] Lock %s not found or already released",
			req.LockId,
		)
		return false
	}
	return true
}

func (s *LockServiceServer) isLockedByReqOwner(req *api.ReleaseRequest) bool {
	lockInfo := s.locked.GetLockInfo(req.LockId)

	if req.OwnerId != lockInfo.Owner {
		s.logger.Warnf(
			"[Release] Owner mismatch for lock %s: expected=%s got=%s",
			req.LockId,
			lockInfo.Owner,
			req.OwnerId,
		)
		return false
	}
	return true
}

func (s *LockServiceServer) releaseLock(req *api.ReleaseRequest) {
	s.retrier.EnqueueRetriesForLock(req.LockId)
	s.locked.RemoveLock(req.LockId)

	s.logger.Infof(
		"[Release] Lock %s released by owner %s",
		req.LockId,
		req.OwnerId,
	)
}

// endregion

// endregion
