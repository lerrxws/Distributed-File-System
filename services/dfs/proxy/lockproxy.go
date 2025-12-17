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

	view     []string
	viewId   int64
	sequence int64

	logger seelog.LoggerInterface

	grpc *grpc.Server

	lock.UnimplementedLockServiceServer
}

func NewLockProxyService(grpcServer *grpc.Server, replica replica.ReplicaServiceClient, primaryAddr string, logger seelog.LoggerInterface) *LockProxyServiceServer {
	s := &LockProxyServiceServer{
		grpc:        grpcServer,
		replica:     replica,
		primaryAddr: primaryAddr,

		viewId:   -1,
		view:     []string{primaryAddr},
		sequence: -1,

		logger: logger,
	}

	s.updateView(context.Background())

	return s
}

func (s *LockProxyServiceServer) Acquire(ctx context.Context, req *lock.AcquireRequest) (*lock.AcquireResponse, error) {
	reqAcq := buildAcquireRequest(req, s.viewId, s.sequence)

	resp, err := s.makeRequest(ctx, reqAcq)
	if err != nil {
		s.logger.Errorf("[LockProxy] Error occurred: %s", err)
		return &lock.AcquireResponse{Success: proto.Bool(false)}, err
	}

	return &lock.AcquireResponse{Success: proto.Bool(resp)}, nil
}

func (s *LockProxyServiceServer) Release(ctx context.Context, req *lock.ReleaseRequest) (*lock.ReleaseResponse, error) {
	reqRel := buildReleaseRequest(req, s.viewId, s.sequence)

	_, err := s.makeRequest(ctx, reqRel)
	if err != nil {
		s.logger.Errorf("[LockProxy] Error occurred: %s", err)
		return &lock.ReleaseResponse{}, err
	}

	return &lock.ReleaseResponse{}, nil
}

func (s *LockProxyServiceServer) Stop(ctx context.Context, req *lock.StopRequest) (*lock.StopResponse, error) {
	reqStop := buildStopRequest(req, s.viewId, s.sequence)

	_, err := s.makeRequest(ctx, reqStop)
	if err != nil {
		s.logger.Errorf("[LockProxy] Error occurred: %s", err)
	}

	return &lock.StopResponse{}, nil
}

// region private methods

func (s *LockProxyServiceServer) makeRequest(ctx context.Context, req *replica.ExecuteMethodRequest) (bool, error) {
	attempt := 0

	for {
		attempt++
		s.logger.Infof("[LockProxy] Attempt #%d: trying to execute method on primary %s", attempt, s.primaryAddr)

		if len(s.view) == 0 {
			s.logger.Errorf("[LockProxy] View is empty, cannot proceed")
			return false, fmt.Errorf("no replicas available in view")
		}

		resp, err := s.tryExecuteMethod(ctx, req)

		if err != nil {
			s.logger.Warnf("[LockProxy] Primary node %s did not respond: %v", s.primaryAddr, err)

			s.deleteFromView(s.primaryAddr)

			if len(s.view) == 0 {
				s.logger.Errorf("[LockProxy] All replicas are unavailable")
				return false, fmt.Errorf("all replicas are unavailable")
			}

			newPrimaryAddr := s.view[0]
			s.logger.Infof("[LockProxy] Switching to new primary: %s", newPrimaryAddr)

			if err := s.updatePrimaryClient(newPrimaryAddr); err != nil {
				s.logger.Errorf("[LockProxy] Failed to connect to new primary %s: %v", newPrimaryAddr, err)
				s.deleteFromView(newPrimaryAddr)
			}

			time.Sleep(100 * time.Millisecond)
			continue
		}

		if !resp.IsPrimary {
			s.logger.Warnf("[LockProxy] Response from NON-PRIMARY node %s", s.primaryAddr)

			if err := s.updateView(ctx); err != nil {
				s.logger.Errorf("[LockProxy] Failed to update view from %s: %v", s.primaryAddr, err)

				s.deleteFromView(s.primaryAddr)

				if len(s.view) > 0 {
					newPrimaryAddr := s.view[0]
					s.updatePrimaryClient(newPrimaryAddr)
				}

				time.Sleep(100 * time.Millisecond)
				continue
			}

			s.logger.Infof("[LockProxy] View updated: viewId=%d, new primary=%s", s.viewId, s.primaryAddr)

			if err := s.updatePrimaryClient(s.primaryAddr); err != nil {
				s.logger.Errorf("[LockProxy] Failed to connect to updated primary %s: %v", s.primaryAddr, err)
				s.deleteFromView(s.primaryAddr)
			}

			time.Sleep(100 * time.Millisecond)
			continue
		}

		s.logger.Infof("[LockProxy] Successfully received response from PRIMARY node %s", s.primaryAddr)

		if resp.ReturnValue != nil {
			return parseBoolFromString(*resp.ReturnValue), nil
		}

		return true, nil
	}
}

func (s *LockProxyServiceServer) tryExecuteMethod(ctx context.Context, exeReq *replica.ExecuteMethodRequest) (*replica.ExecuteMethodResponse, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resp, err := s.replica.ExecuteMethod(ctxWithTimeout, exeReq)
	if err != nil {
		return nil, err
	}

	if resp == nil {
		return nil, fmt.Errorf("received nil response")
	}

	return resp, nil
}

// region view methods

func (s *LockProxyServiceServer) updateView(ctx context.Context) error {
	// ctxWithTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
	// defer cancel()

	replicaResp, err := s.replica.GetView(context.Background(), &replica.GetViewRequest{})
	if err != nil {
		return fmt.Errorf("failed to get view: %w", err)
	}

	if len(replicaResp.View) == 0 {
		return fmt.Errorf("received empty view")
	}

	s.logger.Infof("[LockProxy] View updated: view: %v → %v",
		s.viewId, replicaResp.ViewId, s.view, replicaResp.View)

	s.view = replicaResp.View
	// s.viewId = replicaResp.ViewId
	s.primaryAddr = s.view[0]

	return nil
}

func (s *LockProxyServiceServer) deleteFromView(nodeId string) {
	s.logger.Infof("[LockProxy] Removing node %s from local view", nodeId)

	var updatedView []string

	for _, addr := range s.view {
		if addr != nodeId {
			updatedView = append(updatedView, addr)
		}
	}

	s.view = updatedView

	s.logger.Infof("[LockProxy] Updated local view: %v", s.view)
}

func (s *LockProxyServiceServer) updatePrimaryClient(primaryAddr string) error {
	if primaryAddr == "" {
		return fmt.Errorf("primary address is empty")
	}

	s.primaryAddr = primaryAddr

	s.logger.Infof("[LockProxy] Attempting to connect to primary replica at %s", s.primaryAddr)

	newReplicaClient, err := utils.ConnectToReplicaClient(s.primaryAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to primary replica at %s: %w", s.primaryAddr, err)
	}

	s.replica = newReplicaClient
	s.logger.Infof("[LockProxy] Successfully connected to primary replica at %s", s.primaryAddr)

	return nil
}

// endregion

// region helper functions

func parseBoolFromString(s string) bool {
	return s == "true"
}

// endregion

// endregion
