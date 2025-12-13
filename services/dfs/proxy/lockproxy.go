package proxy

import (
	"context"
	"fmt"
	"time"

	utils "dfs/utils"

	lock "dfs/proto-gen/lock"
	replica "dfs/proto-gen/replica"

	seelog "github.com/cihub/seelog"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type LockProxyServiceServer struct {
	replica     replica.ReplicaServiceClient
	primaryAddr string

	view    []string
	viewId  int64
	retries int

	logger seelog.LoggerInterface

	grpc *grpc.Server

	lock.UnimplementedLockServiceServer
}

func NewLockProxyService(grpcServer *grpc.Server, replica replica.ReplicaServiceClient, primaryAddr string, logger seelog.LoggerInterface) *LockProxyServiceServer {
	s := &LockProxyServiceServer{
		grpc:        grpcServer,
		replica:     replica,
		primaryAddr: primaryAddr,

		viewId:  1,
		view:    []string{primaryAddr},
		retries: 5,

		logger: logger,
	}

	s.updateView(context.Background())

	return s
}

func (s *LockProxyServiceServer) Acquire(ctx context.Context, req *lock.AcquireRequest) (*lock.AcquireResponse, error) {
	reqAcq := buildAcquireRequest(req, s.viewId)

	resp, err := s.makeRequest(ctx, reqAcq)
	if err != nil {
		s.logger.Errorf("[LockProxy] Error occured: %s", err)
	}

	return &lock.AcquireResponse{Success: proto.Bool(resp)}, nil
}

func (s *LockProxyServiceServer) Release(ctx context.Context, req *lock.ReleaseRequest) (*lock.ReleaseResponse, error) {
	reqAcq := buildReleaseRequest(req, s.viewId)

	_, err := s.makeRequest(ctx, reqAcq)
	if err != nil {
		s.logger.Errorf("[LockProxy] Error occured: %s", err)
	}

	return &lock.ReleaseResponse{}, nil
}

func (s *LockProxyServiceServer) Stop(ctx context.Context, req *lock.StopRequest) (*lock.StopResponse, error) {
	reqAcq := buildStopRequest(req, s.viewId)

	_, err := s.makeRequest(ctx, reqAcq)
	if err != nil {
		s.logger.Errorf("[LockProxy] Error occured: %s", err)
	}

	return &lock.StopResponse{}, nil
}

// region private methods

func (s *LockProxyServiceServer) makeRequest(ctx context.Context, req *replica.ExecuteMethodRequest) (bool, error) {
	for len(s.view) != 0 {
		resp, err := s.tryExecuteMethodWithRetries(ctx, req)
		if err != nil {
			s.logger.Errorf("[LockProxy] Primary Node %s do not answer or return Error.", s.primaryAddr)

			s.deleteFromView(s.primaryAddr)

			newPrimaryAddr := s.view[0]
			s.updatePrimaryClient(newPrimaryAddr)
			
			s.updateView(ctx)

			continue
		}

		if !resp.IsPrimary {
			s.logger.Warnf("[LockProxy] Get response NONPRIMARY node")

			s.updateView(ctx)
			s.updatePrimaryClient(s.primaryAddr)

			continue
		}

		s.logger.Infof("[LockProxy] Get response PRIMARY node")
		return true, nil
	}

	return false, nil
}

func (s *LockProxyServiceServer) tryExecuteMethodWithRetries(ctx context.Context, exeReq *replica.ExecuteMethodRequest) (*replica.ExecuteMethodResponse, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()

	var resp *replica.ExecuteMethodResponse
	var err error

	for attempt := 0; attempt < s.retries; attempt++ {
		resp, err = s.replica.ExecuteMethod(ctxWithTimeout, exeReq)

		if err == nil && resp != nil {
			return resp, nil
		}

		if err != nil {
			s.logger.Criticalf("[LockProxy] Error occurred (attempt %d/%d): %s", attempt+1, s.retries, err)
		} else if resp == nil {
			s.logger.Criticalf("[LockProxy] No response received (attempt %d/%d)", attempt+1, s.retries)
		}

		time.Sleep(time.Second * time.Duration(attempt+1))
	}

	return resp, s.logger.Errorf("[LockProxy]  Failed to execute method after %d retries", s.retries)
}

// region view methods

func (s *LockProxyServiceServer) updateView(ctx context.Context) error {
	replicaResp, err := s.replica.GetView(ctx, &replica.GetViewRequest{})
	if err != nil {
		return s.logger.Errorf("[LockProxy] Error ocurred: %s", err)
	}

	s.view = replicaResp.View
	s.viewId = replicaResp.ViewId
	s.primaryAddr = s.view[0]

	return nil
}

func (s *LockProxyServiceServer) deleteFromView(nodeId string) {
	var updatedView []string

	for _, addr := range s.view {
		if addr != nodeId {
			updatedView = append(updatedView, addr)
		}
	}

	s.view = updatedView
}

func (s *LockProxyServiceServer) updatePrimaryClient(primaryAddr string) error {
	if primaryAddr == "" {
		s.logger.Errorf("[LockProxy] Cannot update primary client: primary address is empty")
		return fmt.Errorf("primary address is empty")
	}

	s.primaryAddr = primaryAddr

	s.logger.Infof("[LockProxy] Attempting to connect to primary replica at %s", s.primaryAddr)

	newReplicaClient, err := utils.ConnectToReplicaClient(s.primaryAddr)
	if err != nil {
		s.logger.Errorf("[LockProxy] Failed to connect to primary replica at %s: %v", s.primaryAddr, err)
		return err
	}

	s.replica = newReplicaClient
	s.logger.Infof("[LockProxy] Successfully connected to primary replica at %s", s.primaryAddr)

	return nil
}

// endregion

// endregion
