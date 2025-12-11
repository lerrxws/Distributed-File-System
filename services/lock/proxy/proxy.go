package proxy

import (
	"context"
	"strconv"

	lock "dfs/proto-gen/lock"
	replica "dfs/proto-gen/replica"

	seelog "github.com/cihub/seelog"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type LockProxyServiceServer struct {
	replica     replica.ReplicaServiceClient
	primaryAddr string
	view        []string
	viewId      int64

	logger seelog.LoggerInterface

	grpc *grpc.Server
	lock.UnimplementedLockServiceServer
}

func NewLockProxyService(grpcServer *grpc.Server, replica replica.ReplicaServiceClient) *LockProxyServiceServer {
	s := &LockProxyServiceServer{
		grpc:    grpcServer,
		replica: replica,
	}

	if err := s.updateView(context.Background()); err != nil {
		s.logger.Criticalf("[LockProxy] Error occurred during view update: %s", err)
	}

	return s
}

func (s *LockProxyServiceServer) Acquire(ctx context.Context, req *lock.AcquireRequest) (*lock.AcquireResponse, error) {
	reqAcq := buildAcquireRequest(req, s.viewId)

	resp, err := s.replica.ExecuteMethod(ctx, reqAcq)
	if err != nil {
		s.logger.Errorf("[LockProxy] Error occured: %s", err)
		return &lock.AcquireResponse{Success: proto.Bool(false)}, nil
	}

	if resp.ReturnValue == nil {
		s.logger.Errorf("[LockProxy] Unable to process return value. Return Value is nil")
		return &lock.AcquireResponse{Success: proto.Bool(false)}, nil
	}

	respBool, err := strconv.ParseBool(*resp.ReturnValue)
	if err != nil {
		s.logger.Errorf("[LockProxy] Unable to process return value. Error while converting %s to bool.", *resp.ReturnValue)
		return &lock.AcquireResponse{Success: proto.Bool(false)}, nil
	}

	return &lock.AcquireResponse{Success: proto.Bool(respBool)}, nil
}

func (s *LockProxyServiceServer) Release(ctx context.Context, req *lock.ReleaseRequest) (*lock.ReleaseResponse, error) {
	reqAcq := buildReleaseRequest(req, s.viewId)

	_, err := s.replica.ExecuteMethod(ctx, reqAcq)
	if err != nil {
		s.logger.Errorf("[LockProxy] Error occured: %s", err)
		return &lock.ReleaseResponse{}, nil
	}

	return &lock.ReleaseResponse{}, nil
}

func (s *LockProxyServiceServer) Stop(ctx context.Context, req *lock.StopRequest) (*lock.StopResponse, error) {
	reqAcq := buildStopRequest(req, s.viewId)

	_, err := s.replica.ExecuteMethod(ctx, reqAcq)
	if err != nil {
		s.logger.Errorf("[LockProxy] Error occured: %s", err)
	}

	return &lock.StopResponse{}, nil
}

// region private methods

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

// endregion

// endregion
