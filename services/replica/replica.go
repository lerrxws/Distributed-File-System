package replica

import (
	"context"
	"strconv"
	"time"

	lockapi "dfs/proto-gen/lock"
	managementApi "dfs/proto-gen/management"
	paxosApi "dfs/proto-gen/paxos"
	replicaApi "dfs/proto-gen/replica"

	paxos "dfs/services/replica/paxos"

	"github.com/cihub/seelog"
	"google.golang.org/grpc"
)

const (
	Acquire string = "Acquire"
	Release string = "Release"
	Stop    string = "Stop"
)

type ReplicaServiceServer struct {
	grpc *grpc.Server

	addr           string
	isPrimary      bool
	lockClient     lockapi.LockServiceClient
	methodExecuted []*replicaApi.MethodRequest
	isRecovered    bool

	viewId            int64
	view              []string
	heartbeatInterval time.Duration

	proposer paxos.Proposer
	acceptor paxos.Acceptor

	logger seelog.LoggerInterface

	replicaApi.UnimplementedReplicaServiceServer
	managementApi.ManagementServiceServer
	paxosApi.UnimplementedPaxosServiceServer
}

func NewReplicaServiceServer(
	grpc *grpc.Server,
	lockClient lockapi.LockServiceClient,
	addr string,
	logger seelog.LoggerInterface,

	primaryAddr string,
) *ReplicaServiceServer {
	r := &ReplicaServiceServer{
		grpc:           grpc,
		lockClient:     lockClient,
		methodExecuted: []*replicaApi.MethodRequest{},
		isRecovered:    false,

		addr:   addr,
		logger: logger,

		view:              []string{addr},
		viewId:            1,
		heartbeatInterval: 5 * time.Second,
	}

	r.acceptor = *paxos.NewAcceptor(r.viewId, r.view, r.commitView, r.logger)
	r.proposer = *paxos.NewProposer(r.commitView, logger)

	if !isPrimaryAddressSet(primaryAddr) {
		r.isPrimary = true
		r.isRecovered = true
	} else {
		r.joinView(primaryAddr)
	}

	go r.monitorViewHealth()

	return r
}

// region replica module

func (r *ReplicaServiceServer) GetView(ctx context.Context, req *replicaApi.GetViewRequest) (*replicaApi.GetViewResponse, error) {
	r.logger.Infof("[Replica] Received GetView request.")

	return &replicaApi.GetViewResponse{
		ViewId: r.viewId,
		View:   r.view,
	}, nil
}

func (s *ReplicaServiceServer) ExecuteMethod(ctx context.Context, req *replicaApi.ExecuteMethodRequest) (*replicaApi.ExecuteMethodResponse, error) {
	if !s.isPrimary {
		s.logger.Warnf("[Lock Replica] Lock Replica (%s) is not a primary node.", s.addr)
		return &replicaApi.ExecuteMethodResponse{
			IsPrimary: s.isPrimary,
		}, nil
	}

	methodName := req.MethodRequest.MethodName
	methodParams := req.MethodRequest.MethodParameters

	var resp *replicaApi.ExecuteMethodResponse
	var err error

	switch methodName {
	case Acquire:
		s.logger.Errorf("[Lock Replica] Get Acquire request.")
		resp, err = s.handleAcquire(ctx, methodParams)

	case Release:
		s.logger.Errorf("[Lock Replica] Get Release request.")
		resp, err = s.handleRelease(ctx, methodParams)

	case Stop:
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

func (s *ReplicaServiceServer) Stop(context.Context, *replicaApi.StopRequest) (*replicaApi.StopResponse, error) {
	s.logger.Infof("[Lock Replica] Received stop request — initiating graceful shutdown...")

	go func() {
		s.grpc.GracefulStop()
		s.logger.Infof("[Lock Replica] gRPC lockapiServer stopped successfully")
	}()

	return &replicaApi.StopResponse{}, nil
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
		return nil, s.logger.Errorf("[Lock Replica] Failed to parse sequence: %v", err)
	}

	resp, err := s.lockClient.Acquire(ctx, &lockapi.AcquireRequest{
		LockId:   lockId,
		OwnerId:  ownerId,
		Sequence: sequenceInt64,
	})
	if err != nil {
		s.logger.Errorf("[Lock Replica] Error occurred while processing the %s request: %s.", Acquire, err)
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

	_, err = s.lockClient.Release(ctx, &lockapi.ReleaseRequest{
		LockId:   lockId,
		OwnerId:  ownerId,
		Sequence: sequenceInt64,
	})
	if err != nil {
		s.logger.Errorf("[Lock Replica] Error occurred while processing the %s request: %s.", Release, err)
		return &replicaApi.ExecuteMethodResponse{IsPrimary: s.isPrimary}, nil
	}

	return &replicaApi.ExecuteMethodResponse{IsPrimary: s.isPrimary}, nil
}

func (s *ReplicaServiceServer) handleStop(ctx context.Context) (*replicaApi.ExecuteMethodResponse, error) {
	_, err := s.lockClient.Stop(ctx, &lockapi.StopRequest{})
	if err != nil {
		s.logger.Errorf("[Lock Replica] Error occurred while processing the %s request: %s.", Stop, err)
		return &replicaApi.ExecuteMethodResponse{IsPrimary: s.isPrimary}, nil
	}

	return &replicaApi.ExecuteMethodResponse{IsPrimary: s.isPrimary}, nil
}

func (s *ReplicaServiceServer) handleUnknownMethod(methodName string) (*replicaApi.ExecuteMethodResponse, error) {
	return &replicaApi.ExecuteMethodResponse{IsPrimary: s.isPrimary}, nil
}

// endregion

// region management module

func (r *ReplicaServiceServer) Heartbeat(ctx context.Context, req *managementApi.HeartbeatRequest) (*managementApi.HeartbeatResponse, error) {
	r.logger.Infof("[Management] Get HeartBeat from node %s.", req.Sender)
	return &managementApi.HeartbeatResponse{}, nil
}

func (r *ReplicaServiceServer) monitorViewHealth() {
	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		r.logger.Infof("[Management] Starting new heartbeat cycle for view: %v", r.view)

		for _, node := range r.view {
			r.sendHeartbeat(node)
		}
	}
}

func (r *ReplicaServiceServer) sendHeartbeat(node string) {
	client, err := connectToManagementClient(node)
	if err != nil {
		r.removeFromView(node)
	}

	r.logger.Infof("[Management] Attempting to send heartbeat to node: %s", node)

	_, err = client.Heartbeat(context.Background(), &managementApi.HeartbeatRequest{
		Sender: r.addr,
	})

	if err != nil {
		r.logger.Errorf("[Management] Failed to connect to node %s: %v", node, err)

		r.removeFromView(node)
	}
}

func (r *ReplicaServiceServer) commitView(viewId int64, view []string) {
	r.logger.Infof("[Management] Commiting new view (id: %d, value: %s).", viewId, view)
	r.viewId = viewId
	r.view = view

	if r.checkIsPrimary() {
		r.logger.Infof("[Management] This node is now the primary node: %s", r.addr)
		r.isPrimary = true
	}
}

func (r *ReplicaServiceServer) joinView(primaryAddr string) {
	r.logger.Infof("[Management] Attempting to connect to primary node %s.", primaryAddr)

	replicaClient, err := connectToReplicaClient(primaryAddr)
	if err != nil {
		r.logger.Infof("[Management] Error while connecting to primary node %s. Becoming the primary node.", primaryAddr)
		r.isPrimary = true
		return
	}

	r.logger.Infof("[Management] Attempting to synchronize view with primary node %s.", primaryAddr)
	resp, err := replicaClient.GetView(context.Background(), &replicaApi.GetViewRequest{})
	if err != nil {
		r.logger.Infof("[Management] Error while synchronizing view with primary node %s. Becoming the primary node.", primaryAddr)
		r.isPrimary = true
		return
	}

	r.logger.Infof("[Management] View synchronized with primary node %s. Attempting to add node %s to the view.", primaryAddr, r.addr)

	newViewId := resp.ViewId + 1
	newView := append(resp.View, r.addr)

	r.logger.Infof("[Management] Proposing new view with ViewId %d: %v", newViewId, newView)
	go r.proposer.Propose(newViewId, newView, newView)
}

func (r *ReplicaServiceServer) removeFromView(nodeAddr string) {
	r.logger.Infof("[Management] Attempting to remove node %s from the view.", nodeAddr)

	newViewId := r.viewId + 1
	newView := []string{}

	for _, node := range r.view {
		if node != nodeAddr {
			newView = append(newView, node)
		}
	}

	r.logger.Infof("[Management] Node %s removed from the view. New view: %v", nodeAddr, newView)

	r.logger.Infof("[Management] Proposing new view with ViewId %d: %v", newViewId, newView)

	go r.proposer.Propose(newViewId, newView, newView)
}

func (r *ReplicaServiceServer) checkIsPrimary() bool {
	return r.view[0] == r.addr
}

// endregion

// region paxos module

func (r *ReplicaServiceServer) Prepare(ctx context.Context, req *paxosApi.PrepareRequest) (*paxosApi.PrepareResponse, error) {
	r.logger.Infof("[Replica] Received Prepare request.")
	return r.acceptor.Prepare(req)
}

func (r *ReplicaServiceServer) Accept(ctx context.Context, req *paxosApi.AcceptRequest) (*paxosApi.AcceptResponse, error) {
	r.logger.Infof("[Replica] Received Access request.")
	return r.acceptor.Accept(req)
}

func (r *ReplicaServiceServer) Decide(ctx context.Context, req *paxosApi.DecideRequest) (*paxosApi.DecideResponse, error) {
	r.logger.Infof("[Replica] Received Decide request.")
	resp, err := r.acceptor.Decide(req)

	return resp, err
}

// endregion
