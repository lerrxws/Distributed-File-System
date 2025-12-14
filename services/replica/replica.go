package replica

import (
	"context"
	"strconv"

	lockapi "dfs/proto-gen/lock"
	replicaApi "dfs/proto-gen/replica"
)

const (
	Acquire string = "Acquire"
	Release string = "Release"
	Stop    string = "Stop"
)

func (r *ReplicaServiceServer) GetView(ctx context.Context, req *replicaApi.GetViewRequest) (*replicaApi.GetViewResponse, error) {
	r.logger.Infof("[Replica] Received GetView request.")

	return &replicaApi.GetViewResponse{
		ViewId: r.manager.ViewId,
		View:   r.manager.View,
	}, nil
}

func (s *ReplicaServiceServer) ExecuteMethod(ctx context.Context, req *replicaApi.ExecuteMethodRequest) (*replicaApi.ExecuteMethodResponse, error) {
	if req.Viewstamp.ViewId != s.manager.ViewId {
		s.logger.Warnf("[Replica] Incorrect viewId %d (current viewId is %d). Unable to execute method.", req.Viewstamp.ViewId, s.manager.ViewId)
		return &replicaApi.ExecuteMethodResponse{
			IsPrimary: s.manager.IsPrimary,
		}, nil
	}

	if !s.isRecovered {
		s.logger.Warnf("[Replica] Replica (%s) is not recovered. Cannot execute methods until recovery is complete.", s.manager.Addr)
		return &replicaApi.ExecuteMethodResponse{
			IsPrimary: s.manager.IsPrimary,
		}, nil
	}

	if s.manager.IsPrimary {
		viewWithoutPrimaryAddr := s.manager.GetViewWithoutOwnAddr()
		synchReq := SynchronizeRequest{
			methodReq: req,
			view:      viewWithoutPrimaryAddr,
		}
		s.synchronizer.AddMethodExecutionToQueue(synchReq)
	}

	return s.handleExecuteMethod(ctx, req.MethodRequest)
}

func (s *ReplicaServiceServer) GetExecutedMethods(ctx context.Context, req *replicaApi.GetExecutedMethodsRequest) (*replicaApi.GetExecutedMethodsResponse, error) {
	s.logger.Infof("[Replica] Received GetExecutedMethods request.")

	return &replicaApi.GetExecutedMethodsResponse{
		MethodRequests: s.methodExecuted,
	}, nil
}

func (s *ReplicaServiceServer) IsRecovered(ctx context.Context, req *replicaApi.IsRecoveredRequest) (*replicaApi.IsRecoveredResponse, error) {
	s.logger.Infof("[Replica] Received GetExecutedMethods request.")
	return &replicaApi.IsRecoveredResponse{
		IsRecovered: s.isRecovered,
	}, nil
}

func (s *ReplicaServiceServer) Stop(context.Context, *replicaApi.StopRequest) (*replicaApi.StopResponse, error) {
	s.logger.Infof("[Replica] Received stop request — initiating graceful shutdown...")

	s.synchronizer.Stop()

	go func() {
		s.grpc.GracefulStop()
		s.logger.Infof("[Replica] gRPC lockapiServer stopped successfully")
	}()

	return &replicaApi.StopResponse{}, nil
}

// region help functions

func (s *ReplicaServiceServer) handleExecuteMethod(ctx context.Context, methodReq *replicaApi.MethodRequest) (*replicaApi.ExecuteMethodResponse, error) {
	methodName := methodReq.MethodName
	methodParams := methodReq.MethodParameters

	var resp *replicaApi.ExecuteMethodResponse
	var err error

	switch methodName {
	case Acquire:
		s.logger.Errorf("[Replica] Get Acquire request.")
		resp, err = s.handleAcquire(ctx, methodParams)

	case Release:
		s.logger.Errorf("[Replica] Get Release request.")
		resp, err = s.handleRelease(ctx, methodParams)

	case Stop:
		s.logger.Errorf("[Replica] Get Stop request.")
		resp, err = s.handleStop(ctx)

	default:
		s.logger.Errorf("[Replica] Unknown method name %s.", methodName)
		resp, err = s.handleUnknownMethod(methodName)
	}

	if err == nil && resp != nil && resp.IsPrimary {
		s.methodExecuted = append(s.methodExecuted, methodReq)

		err = s.logToFileExecutedMethod(methodReq)
		if err != nil {
			s.logger.Errorf(
				"[Replica] Failed to log executed method '%s' with parameters %v: %v",
				methodReq.MethodName,
				methodReq.MethodParameters,
				err,
			)
		}
	}

	return resp, err
}

func (s *ReplicaServiceServer) handleAcquire(ctx context.Context, methodParams []string) (*replicaApi.ExecuteMethodResponse, error) {
	if len(methodParams) < 3 {
		return nil, s.logger.Errorf("invalid parameters for Acquire")
	}

	lockId := methodParams[0]
	ownerId := methodParams[1]
	sequence := methodParams[2]

	sequenceInt64, err := strconv.ParseInt(sequence, 10, 64)
	if err != nil {
		return nil, s.logger.Errorf("[Replica] Failed to parse sequence: %v", err)
	}

	resp, err := s.lockClient.Acquire(ctx, &lockapi.AcquireRequest{
		LockId:   lockId,
		OwnerId:  ownerId,
		Sequence: sequenceInt64,
	})
	if err != nil {
		s.logger.Errorf("[Replica] Error occurred while processing the %s request: %s.", Acquire, err)
		return &replicaApi.ExecuteMethodResponse{IsPrimary: s.manager.IsPrimary}, nil
	}

	respStr := strconv.FormatBool(*resp.Success)
	return &replicaApi.ExecuteMethodResponse{
		IsPrimary:   s.manager.IsPrimary,
		ReturnValue: &respStr,
	}, nil
}

func (s *ReplicaServiceServer) handleRelease(ctx context.Context, methodParams []string) (*replicaApi.ExecuteMethodResponse, error) {

	if len(methodParams) < 3 {
		return nil, s.logger.Errorf("[Replica] Invalid parameters for Release")
	}

	lockId := methodParams[0]
	ownerId := methodParams[1]
	sequence := methodParams[2]

	sequenceInt64, err := strconv.ParseInt(sequence, 10, 64)
	if err != nil {
		return nil, s.logger.Errorf("[Replica] Failed to parse sequence: %v", err)
	}

	_, err = s.lockClient.Release(ctx, &lockapi.ReleaseRequest{
		LockId:   lockId,
		OwnerId:  ownerId,
		Sequence: sequenceInt64,
	})
	if err != nil {
		s.logger.Errorf("[Replica] Error occurred while processing the %s request: %s.", Release, err)
		return &replicaApi.ExecuteMethodResponse{IsPrimary: s.manager.IsPrimary}, nil
	}

	return &replicaApi.ExecuteMethodResponse{IsPrimary: s.manager.IsPrimary}, nil
}

func (s *ReplicaServiceServer) handleStop(ctx context.Context) (*replicaApi.ExecuteMethodResponse, error) {
	_, err := s.lockClient.Stop(ctx, &lockapi.StopRequest{})
	if err != nil {
		s.logger.Errorf("[Replica] Error occurred while processing the %s request: %s.", Stop, err)
		return &replicaApi.ExecuteMethodResponse{IsPrimary: s.manager.IsPrimary}, nil
	}

	return &replicaApi.ExecuteMethodResponse{IsPrimary: s.manager.IsPrimary}, nil
}

func (s *ReplicaServiceServer) handleUnknownMethod(methodName string) (*replicaApi.ExecuteMethodResponse, error) {
	return &replicaApi.ExecuteMethodResponse{IsPrimary: s.manager.IsPrimary}, nil
}

// endregion