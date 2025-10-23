package dfs

import (
	"context"

	lockapi "dfs/proto-gen/lock"

	seelog "github.com/cihub/seelog"
)

type ReleaserTask struct {
	releaseQueue chan *CacheInfo
	cacheManager *CacheManager
	logger       seelog.LoggerInterface
	stopCh       chan struct{}
}

func NewReleaser(cacheManager *CacheManager, logger seelog.LoggerInterface) *ReleaserTask {
	return &ReleaserTask{
		releaseQueue: make(chan *CacheInfo, 100),
		cacheManager: cacheManager,
		logger:       logger,
		stopCh:       make(chan struct{}),
	}
}

func (r *ReleaserTask) Start() {
	r.logger.Infof("[Releaser] Starting release task...")

	go func() {
		for {
			select {
			case cache, ok := <-r.releaseQueue:
				if !ok {
					r.logger.Infof("[Releaser] Queue closed — stopping worker.")
					return
				}
				r.handleRelease(cache)

			case <-r.stopCh:
				r.logger.Infof("[Releaser] Stop signal received.")
				return
			}
		}
	}()

	r.logger.Infof("[Releaser] Task successfully started.")
}

func (r *ReleaserTask) AddTask(cache *CacheInfo) {
	select {
	case r.releaseQueue <- cache:
		r.logger.Debugf("[Releaser] Added release task for lock=%s", cache.LockId)
	default:
		r.logger.Warnf("[Releaser] Queue full — dropping task for lock=%s", cache.LockId)
	}
}

func (r *ReleaserTask) Stop() {
	close(r.stopCh)
	close(r.releaseQueue)
}

func (r *ReleaserTask) handleRelease(cacheInfo *CacheInfo) {
	r.logger.Debugf("[Releaser] Processing release task: lock=%s, owner=%s, seq=%d",
		cacheInfo.LockId, cacheInfo.OwnerId, cacheInfo.SeqNum)	

		cacheInfo.mu.Lock()
		if cacheInfo.State != Releasing {
			r.logger.Tracef("[Releaser] Changing state: %s → Releasing", cacheInfo.State)
			cacheInfo.State = Releasing
		}
		cacheInfo.mu.Unlock()

		resp, err := r.cacheManager.ReleaseRPC(context.Background(), &lockapi.ReleaseRequest{
		LockId:   cacheInfo.LockId,
		OwnerId:  cacheInfo.OwnerId,
		Sequence: cacheInfo.SeqNum,
	})

	if err != nil {
		r.logger.Errorf("[Releaser] Failed to release lock %s: %v", cacheInfo.LockId, err)
		return
	}

	r.logger.Infof("[Releaser] Lock %s released successfully (server response: %+v)", cacheInfo.LockId, resp)
}
