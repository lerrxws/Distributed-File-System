package lock

import (
	"context"
	"strings"
	"sync"
	"time"

	lockapi "dfs/proto-gen/lock"
	lcapi "dfs/proto-gen/lockcache"

	seelog "github.com/cihub/seelog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RetryTask struct {
	pending   map[string][]*lockapi.AcquireRequest // lockId -> list of clients waiting
	retryChan chan *lockapi.AcquireRequest
	mu        sync.Mutex
	logger    seelog.LoggerInterface
}

func NewRetryTask(logger seelog.LoggerInterface) *RetryTask {
	r := &RetryTask{
		pending:   make(map[string][]*lockapi.AcquireRequest),
		retryChan: make(chan *lockapi.AcquireRequest, 100),
		logger:    logger,
	}

	logger.Infof("[Retrier] Initialized (queue capacity=%d)", cap(r.retryChan))
	return r
}

func (r *RetryTask) AddClientForRetry(ownerId, lockId string, seqNum int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	req := &lockapi.AcquireRequest{
		LockId:   lockId,
		OwnerId:  ownerId,
		Sequence: seqNum,
	}

	r.pending[lockId] = append(r.pending[lockId], req)
	r.logger.Infof("[Retrier] Added pending client for lock %s (owner=%s, seq=%d, total_waiting=%d)",
		lockId, ownerId, seqNum, len(r.pending[lockId]))
}

func (r *RetryTask) PopPendingForLock(lockId string) []*lockapi.AcquireRequest {
	r.mu.Lock()
	defer r.mu.Unlock()

	reqs, ok := r.pending[lockId]
	if ok {
		delete(r.pending, lockId)
		r.logger.Infof("[Retrier] Retrieved %d pending clients for lock %s", len(reqs), lockId)
		return reqs
	}

	r.logger.Infof("[Retrier] No pending clients for lock %s", lockId)
	return nil
}

func (r *RetryTask) EnqueueRetriesForLock(lockId string) {
	reqs := r.PopPendingForLock(lockId)
	if reqs == nil {
		return
	}

	for _, req := range reqs {
		select {
		case r.retryChan <- req:
			r.logger.Infof("[Retrier] Queued retry for lock %s (owner=%s, seq=%d)",
				req.LockId, req.OwnerId, req.Sequence)
		default:
			r.logger.Warnf("[Retrier] Retry channel full — skipping lock %s (owner=%s)", req.LockId, req.OwnerId)
		}
	}
}

func (r *RetryTask) Start() {
	r.logger.Infof("[Retrier] Starting retry task processor...")

	go func() {
		for req := range r.retryChan {
			r.SendRetry(req)
		}
	}()

	r.logger.Infof("[Retrier] Retry task processor started successfully.")
}

func (r *RetryTask) SendRetry(req *lockapi.AcquireRequest) {
	r.logger.Debugf("[Retrier] Processing retry task: lock=%s, owner=%s", req.LockId, req.OwnerId)

	parts := strings.SplitN(req.OwnerId, ":", 3)
	if len(parts) < 2 {
		r.logger.Errorf("[Retrier] Invalid OwnerId format: %s", req.OwnerId)
		return
	}
	clientAddr := strings.Join(parts[:2], ":")

	clientConn, err := grpc.NewClient(clientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		r.logger.Errorf("[Retrier] Failed to connect to client %s: %v", clientAddr, err)
		return
	}
	defer clientConn.Close()

	client := lcapi.NewLockCacheServiceClient(clientConn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = client.Retry(ctx, &lcapi.RetryRequest{
		LockId:   req.LockId,
		Sequence: req.Sequence,
	})
	if err != nil {
		r.logger.Errorf("[Retrier] Failed to send retry to %s: %v", req.OwnerId, err)
	} else {
		r.logger.Infof("[Retrier] Successfully sent retry to %s for lock %s (seq=%d)",
			req.OwnerId, req.LockId, req.Sequence)
	}
}

func (r *RetryTask) Stop() {
	r.logger.Infof("[Retrier] Stopping retry task...")
	defer func() {
		if rec := recover(); rec != nil {
			r.logger.Warnf("[Retrier] Panic during stop: %v", rec)
		}
		r.logger.Infof("[Retrier] Retry task stopped cleanly.")
	}()
	close(r.retryChan)
}
