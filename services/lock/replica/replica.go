package replica

import (
	"context"
	"strconv"

	lockapi "dfs/proto-gen/lock"
	replicaApi "dfs/proto-gen/replica"

	lock "dfs/services/lock"

	seelog "github.com/cihub/seelog"

	"google.golang.org/grpc"
)

type ReplicaServiceServer struct {
	grpc          *grpc.Server
	lockapiClient lockapi.LockServiceClient

	isPrimary   bool
	nodeId      string
	view        []string
	viewId      int64
	isRecovered bool

	methodExecuted []*replicaApi.MethodRequest

	logger seelog.LoggerInterface

	replicaApi.UnimplementedReplicaServiceServer
}

func NewReplicaServiceServer(grpc *grpc.Server, lockapiClient lockapi.LockServiceClient,
	nodeId string, logger seelog.LoggerInterface) *ReplicaServiceServer {
	s := &ReplicaServiceServer{
		grpc:          grpc,
		lockapiClient: lockapiClient,

		nodeId: nodeId,
		logger: logger,

		isPrimary: true,
	}

	s.view = append(s.view, nodeId)
	s.viewId = 1

	s.methodExecuted = []*replicaApi.MethodRequest{}

	return s
}

func (s *ReplicaServiceServer) Stop(context.Context, *replicaApi.StopRequest) (*replicaApi.StopResponse, error) {
	s.logger.Infof("[Lock Replica] Received stop request — initiating graceful shutdown...")

	go func() {
		s.grpc.GracefulStop()
		s.logger.Infof("[Lock Replica] gRPC lockapiServer stopped successfully")
	}()

	return &replicaApi.StopResponse{}, nil
}

func (s *ReplicaServiceServer) GetView(ctx context.Context, req *replicaApi.GetViewRequest) (*replicaApi.GetViewResponse, error) {
	s.logger.Infof("[Lock Replica] Received GetView request.")

	return &replicaApi.GetViewResponse{
		ViewId: s.viewId,
		View:   s.view,
	}, nil
}

func (s *ReplicaServiceServer) ExecuteMethod(ctx context.Context, req *replicaApi.ExecuteMethodRequest) (*replicaApi.ExecuteMethodResponse, error) {
	if !s.isPrimary {
		s.logger.Warnf("[Lock Replica] Lock Replica (%s) is not a primary node.", s.nodeId)
		return &replicaApi.ExecuteMethodResponse{
			IsPrimary: s.isPrimary,
		}, nil
	}

	methodName := req.MethodRequest.MethodName
	methodParams := req.MethodRequest.MethodParameters

	var resp *replicaApi.ExecuteMethodResponse
	var err error

	switch methodName {
	case lock.Acquire:
		s.logger.Errorf("[Lock Replica] Get Acquire request.")
		resp, err = s.handleAcquire(ctx, methodParams)

	case lock.Release:
		s.logger.Errorf("[Lock Replica] Get Release request.")
		resp, err = s.handleRelease(ctx, methodParams)

	case lock.Stop:
		s.logger.Errorf("[Lock Replica] Get Stop request.")
		resp, err = s.handleStop(ctx)

	default:
		s.logger.Errorf("[Lock Replica] Unknown method name %s.", methodName)
		resp, err = s.handleUnknownMethod(methodName)
	}

	if err == nil && resp != nil && resp.IsPrimary {
		s.methodExecuted = append(s.methodExecuted, req.MethodRequest)
	}

	return resp, err
}

func (s *ReplicaServiceServer) GetExecutedMethods(ctx context.Context, req *replicaApi.GetExecutedMethodsRequest) (*replicaApi.GetExecutedMethodsResponse, error) {
	s.logger.Infof("[Lock Replica] Received GetExecutedMethods request.")

	return &replicaApi.GetExecutedMethodsResponse{
		MethodRequests: s.methodExecuted,
	}, nil
}

func (s *ReplicaServiceServer) IsRecovered(ctx context.Context, req *replicaApi.IsRecoveredRequest) (*replicaApi.IsRecoveredResponse, error) {
	s.logger.Infof("[Lock Replica] Received GetExecutedMethods request.")
	return &replicaApi.IsRecoveredResponse{
		IsRecovered: s.isRecovered,
	}, nil
}

// region private methods

// region handle execute methods

func (s *ReplicaServiceServer) handleAcquire(ctx context.Context, methodParams []string) (*replicaApi.ExecuteMethodResponse, error) {
	if len(methodParams) < 3 {
		return nil, s.logger.Errorf("invalid parameters for Acquire")
	}

	lockId := methodParams[0]
	ownerId := methodParams[1]
	sequence := methodParams[2]

	sequenceInt64, err := strconv.ParseInt(sequence, 10, 64)
	if err != nil {
		return nil, s.logger.Errorf("[Lock Replica] Failed to parse sequence: %v", err)
	}

	resp, err := s.lockapiClient.Acquire(ctx, &lockapi.AcquireRequest{
		LockId:   lockId,
		OwnerId:  ownerId,
		Sequence: sequenceInt64,
	})
	if err != nil {
		s.logger.Errorf("[Lock Replica] Error occurred while processing the %s request: %s.", lock.Acquire, err)
		return &replicaApi.ExecuteMethodResponse{IsPrimary: s.isPrimary}, nil
	}

	respStr := strconv.FormatBool(*resp.Success)
	return &replicaApi.ExecuteMethodResponse{
		IsPrimary:   s.isPrimary,
		ReturnValue: &respStr,
	}, nil
}

func (s *ReplicaServiceServer) handleRelease(ctx context.Context, methodParams []string) (*replicaApi.ExecuteMethodResponse, error) {

	if len(methodParams) < 3 {
		return nil, s.logger.Errorf("[Lock Replica] Invalid parameters for Release")
	}

	lockId := methodParams[0]
	ownerId := methodParams[1]
	sequence := methodParams[2]

	sequenceInt64, err := strconv.ParseInt(sequence, 10, 64)
	if err != nil {
		return nil, s.logger.Errorf("[Lock Replica] Failed to parse sequence: %v", err)
	}

	_, err = s.lockapiClient.Release(ctx, &lockapi.ReleaseRequest{
		LockId:   lockId,
		OwnerId:  ownerId,
		Sequence: sequenceInt64,
	})
	if err != nil {
		s.logger.Errorf("[Lock Replica] Error occurred while processing the %s request: %s.", lock.Release, err)
		return &replicaApi.ExecuteMethodResponse{IsPrimary: s.isPrimary}, nil
	}

	return &replicaApi.ExecuteMethodResponse{IsPrimary: s.isPrimary}, nil
}

func (s *ReplicaServiceServer) handleStop(ctx context.Context) (*replicaApi.ExecuteMethodResponse, error) {
	_, err := s.lockapiClient.Stop(ctx, &lockapi.StopRequest{})
	if err != nil {
		s.logger.Errorf("[Lock Replica] Error occurred while processing the %s request: %s.", lock.Stop, err)
		return &replicaApi.ExecuteMethodResponse{IsPrimary: s.isPrimary}, nil
	}

	return &replicaApi.ExecuteMethodResponse{IsPrimary: s.isPrimary}, nil
}

func (s *ReplicaServiceServer) handleUnknownMethod(methodName string) (*replicaApi.ExecuteMethodResponse, error) {
	return &replicaApi.ExecuteMethodResponse{IsPrimary: s.isPrimary}, nil
}

// endregion

//endregion
