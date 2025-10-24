package lock

import (
	"context"
	"strings"
	"time"

	lockapi "dfs/proto-gen/lock"
	lcapi "dfs/proto-gen/lockcache"

	seelog "github.com/cihub/seelog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RetryTask struct {
	retryQueue chan *lockapi.AcquireRequest
	logger     seelog.LoggerInterface
}

func NewRetryTask(logger seelog.LoggerInterface) *RetryTask {
	return &RetryTask{
		retryQueue: make(chan *lockapi.AcquireRequest, 100),
		logger:     logger,
	}
}

func (r *RetryTask) Start() {
	r.logger.Infof("[Retrier] Starting retry task...")

	go func() {
		for req := range r.retryQueue {
			r.SendRetry(req)
		}
	}()

	r.logger.Infof("[Retrier] Task successfully started.")
}

func (r *RetryTask) AddTask(req *lockapi.AcquireRequest) {
	select {
	case r.retryQueue <- req:
		r.logger.Infof("[Retrier] Queued retry task for lock %s (owner=%s)", req.LockId, req.OwnerId)
	default:
		r.logger.Warnf("[Retrier] Retry queue full — skipping task for lock %s", req.LockId)
	}
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

	_, err = client.Retry(ctx, &lcapi.RetryRequest{LockId: req.LockId, Sequence: req.Sequence})
	if err != nil {
		r.logger.Errorf("[Retrier] Failed to send retry to %s: %v", req.OwnerId, err)
	} else {
		r.logger.Infof("[Retrier] Successfully sent retry to %s for lock %s", req.OwnerId, req.LockId)
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
	close(r.retryQueue)
}
