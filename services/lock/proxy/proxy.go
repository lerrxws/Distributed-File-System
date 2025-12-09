package proxy

import (
	"context"
	"time"

	lock "dfs/proto-gen/lock"
	replica "dfs/proto-gen/replica"

	seelog "github.com/cihub/seelog"
	
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type LockProxyServiceServer struct {
	replica     replica.ReplicaServiceClient
	primaryAddr string
	view        []string
	viewId      int64
	retries     int

	logger seelog.LoggerInterface

	grpc *grpc.Server
	lock.UnimplementedLockServiceServer
}

func NewLockProxyService(grpcServer *grpc.Server, replica replica.ReplicaServiceClient) *LockProxyServiceServer {
	s := &LockProxyServiceServer{
		grpc:    grpcServer,
		replica: replica,
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

func (s *LockProxyServiceServer) connectToReplica(addr string) replica.ReplicaServiceClient {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.logger.Critical("[Lock Proxy] Failed to connect to Lock Replica server at %s: %v", addr, err)
	}
	s.logger.Info("[Lock Proxy] Connected to Lock Replica server at %s", addr)
	return replica.NewReplicaServiceClient(conn)
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

func (s *LockProxyServiceServer) makeRequest(ctx context.Context, req *replica.ExecuteMethodRequest) (bool, error){
	for len(s.view) != 0 {
		resp, err := s.tryExecuteMethodWithRetries(ctx, req)
		if err != nil {
			s.logger.Errorf("[LockProxy] Primary Node do not answer or return Error")

			s.deleteFromView(s.primaryAddr)
			s.updateView(ctx)

			continue
		}

		if !resp.IsPrimary {
			s.logger.Warnf("[LockProxy] Get response NONPRIMARY node")
			s.updateView(ctx)

			continue
		}
			
		s.logger.Infof("[LockProxy] Get response PRIMARY node")
		return true, nil
	}

	return false, nil
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

// endregion

// endregion
