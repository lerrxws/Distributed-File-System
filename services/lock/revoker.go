package lock

import (
	"context"
	"strings"

	lcapi "dfs/proto-gen/lockcache"

	seelog "github.com/cihub/seelog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RevokerTarget struct {
	OwnerId string
	LockId  string
}

type RevokeTask struct {
	revokeQueue chan *RevokerTarget
	logger      seelog.LoggerInterface
}

func NewRevokeTask(logger seelog.LoggerInterface) *RevokeTask {
	r := &RevokeTask{
		revokeQueue: make(chan *RevokerTarget, 100),
		logger:      logger,
	}

	r.logger.Infof("[Revoker] Initialized (queue capacity=%d)", cap(r.revokeQueue))
	return r
}

func (r *RevokeTask) Start() {
	r.logger.Infof("[Revoker] Starting revoke task...")

	go func() {
		for target := range r.revokeQueue {
			r.SendRevoke(target)
		}
	}()

	r.logger.Infof("[Revoker] Task successfully started.")
}

func (r *RevokeTask) AddTask(target *RevokerTarget) {
	select {
	case r.revokeQueue <- target:
		r.logger.Infof("[Revoker] queued revoke task for lock %s (owner %s)", target.LockId, target.OwnerId)
	default:
		r.logger.Warnf("[Revoker] revoke queue full, skipping task for lock %s", target.LockId)
	}
}

func (r *RevokeTask) SendRevoke(target *RevokerTarget) {
	r.logger.Debugf("[Revoker] Processing revoke task: lock=%s, owner=%s", target.LockId, target.OwnerId)

	addrParts := strings.Split(target.OwnerId, ":")
	if len(addrParts) < 2 {
		r.logger.Errorf("[Revoker] invalid ownerId: %s", target.OwnerId)
		return
	}
	clientAddr := strings.Join(addrParts[:2], ":")

	clientConn, err := grpc.NewClient(clientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		r.logger.Errorf("[Revoker] failed to connect to client %s: %v", target.OwnerId, err)
	}
	defer clientConn.Close()

	client := lcapi.NewLockCacheServiceClient(clientConn)

	_, err = client.Revoke(context.Background(), &lcapi.RevokeRequest{LockId: target.LockId})
	if err != nil {
		r.logger.Errorf("[Revoker] failed to send revoke to %s: %v", target.OwnerId, err)
	} else {
		r.logger.Infof("[Revoker] successfully sent revoke to %s for lock %s", target.OwnerId, target.LockId)
	}
}

func (r *RevokeTask) Stop() {
	r.logger.Infof("[Revoker] Stopping revoke task...")
	defer func() {
		recover()
		r.logger.Infof("[Revoker] Revoker stopped cleanly.")
	}()
	close(r.revokeQueue)
}
