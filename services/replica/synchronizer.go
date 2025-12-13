package replica

import (
	"context"
	"fmt"

	utils "dfs/utils"

	replicaApi "dfs/proto-gen/replica"

	"github.com/cihub/seelog"
)

type SynchronizeRequest struct {
	methodReq *replicaApi.ExecuteMethodRequest
	view      []string
}

type Synchronizer struct {
	synchChan chan SynchronizeRequest

	logger seelog.LoggerInterface
}

func NewSyncronizer(logger seelog.LoggerInterface) *Synchronizer {
	s := &Synchronizer{
		synchChan: make(chan SynchronizeRequest, 100),
		logger:    logger,
	}

	return s
}

func (s *Synchronizer) Start() {
	s.logger.Infof("[Synchronizer] Starting synchonize task processor...")

	go func() {
		for req := range s.synchChan {
			s.synchronizeAllReplicas(req)
		}
	}()

	s.logger.Infof("[Synchronizer] Retry task processor started successfully.")
}

func (s *Synchronizer) AddMethodExecutionToQueue(req SynchronizeRequest) {
	s.logger.Infof("[Synchronizer] Adding method execution request to sync queue: %v", req)

	select {
	case s.synchChan <- req:
		s.logger.Infof("[Synchronizer] Method execution request successfully added to queue: %v", req)
	default:
		s.logger.Errorf("[Synchronizer] Failed to add method execution request to queue. Channel is full.")
	}
}

func (s *Synchronizer) Stop() {
	s.logger.Infof("[Retrier] Stopping retry task...")
	defer func() {
		if rec := recover(); rec != nil {
			s.logger.Warnf("[Retrier] Panic during stop: %v", rec)
		}
		s.logger.Infof("[Retrier] Retry task stopped cleanly.")
	}()
	close(s.synchChan)
}

func (s *Synchronizer) synchronizeAllReplicas(req SynchronizeRequest) {
	view := req.view
	methodReq := req.methodReq

	for _, replicaAddr := range view {
		client, err := utils.ConnectToReplicaClient(replicaAddr)
		if err != nil {
			s.logger.Errorf("[Synchronizer] Failed to connect to replica at %s: %v", replicaAddr, err)
			continue
		}

		err = s.executeMethodOnReplica(client, methodReq)
		if err != nil {
			s.logger.Errorf("[Synchronizer] Failed to execute method on replica %s: %v", replicaAddr, err)
			continue
		}

		s.logger.Infof("[Synchronizer] Successfully synchronized method on replica %s", replicaAddr)
	}
	
}

func (s *Synchronizer) executeMethodOnReplica(client replicaApi.ReplicaServiceClient, req *replicaApi.ExecuteMethodRequest) error {
	resp, err := client.IsRecovered(context.Background(), &replicaApi.IsRecoveredRequest{})
	if err != nil {
		s.logger.Errorf("[Synchronizer] Failed to check recovery status: %v", err)
		return err

	}

	if !resp.IsRecovered {
		s.logger.Errorf("[Synchronizer] Replica is not recovered, cannot execute method.")
		return fmt.Errorf("replica is not recovered")
	}

	_, err = client.ExecuteMethod(context.Background(), req)
	if err != nil {
		s.logger.Errorf("[Synchronizer] Failed to execute method on replica: %v", err)
		return err
	}

	s.logger.Infof("[Synchronizer] Method executed successfully on replica")
	return nil
}
