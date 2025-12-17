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

type SynchronizeModule struct {
	synchChan chan SynchronizeRequest

	logger seelog.LoggerInterface
}

func NewSyncronizer(logger seelog.LoggerInterface) *SynchronizeModule {
	s := &SynchronizeModule{
		synchChan: make(chan SynchronizeRequest, 100),
		logger:    logger,
	}

	return s
}

func (s *SynchronizeModule) Start() {
	s.logger.Infof("[SynchronizeModule] Starting synchonize task processor...")

	go func() {
		for req := range s.synchChan {
			s.synchronizeAllReplicas(req)
		}
	}()

	s.logger.Infof("[SynchronizeModule] Retry task processor started successfully.")
}

func (s *SynchronizeModule) AddMethodExecutionToQueue(req SynchronizeRequest) {
	s.logger.Infof("[SynchronizeModule] Adding method execution request to sync queue: %v", req)

	select {
	case s.synchChan <- req:
		s.logger.Infof("[SynchronizeModule] Method execution request successfully added to queue: %v", req)
	default:
		s.logger.Errorf("[SynchronizeModule] Failed to add method execution request to queue. Channel is full.")
	}
}

func (s *SynchronizeModule) Stop() {
	s.logger.Infof("[SynchronizeModule] Stopping retry task...")
	defer func() {
		if rec := recover(); rec != nil {
			s.logger.Warnf("[SynchronizeModule] Panic during stop: %v", rec)
		}
		s.logger.Infof("[SynchronizeModule] Retry task stopped cleanly.")
	}()
	close(s.synchChan)
}

func (s *SynchronizeModule) synchronizeAllReplicas(req SynchronizeRequest) {
	view := req.view[1:]
	methodReq := req.methodReq

	for _, replicaAddr := range view {
		client, err := utils.ConnectToReplicaClient(replicaAddr)
		if err != nil {
			s.logger.Errorf("[SynchronizeModule] Failed to connect to replica at %s: %v", replicaAddr, err)
			continue
		}

		err = s.executeMethodOnReplica(client, methodReq)
		if err != nil {
			s.logger.Errorf("[SynchronizeModule] Failed to execute method on replica %s: %v", replicaAddr, err)
			continue
		}

		s.logger.Infof("[SynchronizeModule] Successfully synchronized method on replica %s", replicaAddr)
	}
	
}

func (s *SynchronizeModule) executeMethodOnReplica(client replicaApi.ReplicaServiceClient, req *replicaApi.ExecuteMethodRequest) error {
	resp, err := client.IsRecovered(context.Background(), &replicaApi.IsRecoveredRequest{})
	if err != nil {
		s.logger.Errorf("[SynchronizeModule] Failed to check recovery status: %v", err)
		return err

	}

	if !resp.IsRecovered {
		s.logger.Errorf("[SynchronizeModule] Replica is not recovered, cannot execute method.")
		return fmt.Errorf("replica is not recovered")
	}

	_, err = client.ExecuteMethod(context.Background(), req)
	if err != nil {
		s.logger.Errorf("[SynchronizeModule] Failed to execute method on replica: %v", err)
		return err
	}

	s.logger.Infof("[SynchronizeModule] Method executed successfully on replica")
	return nil
}
