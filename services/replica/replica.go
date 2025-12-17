package replica

import (
	"context"
	"strconv"
	"sync"

	lockapi "dfs/proto-gen/lock"
	managementApi "dfs/proto-gen/management"
	paxosApi "dfs/proto-gen/paxos"
	replicaApi "dfs/proto-gen/replica"

	utils "dfs/utils"
	fl "dfs/utils/filelogger"

	"github.com/cihub/seelog"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	Acquire string = "Acquire"
	Release string = "Release"
	Stop    string = "Stop"
)

type ReplicaServiceServer struct {
	// identity
	addr      string
	isPrimary bool

	// view state
	viewId int64
	view   []string

	// execution state
	expectedSeqNum int64

	// primary state
	nextSeqNum int64

	// recovery state
	isRecovered bool

	// history
	executedMethods []*replicaApi.MethodRequest

	// business dependency
	lockClient lockapi.LockServiceClient

	// infra
	logger          seelog.LoggerInterface
	filelogger      *fl.JsonFileLogger
	paxosFilelogger *fl.JsonFileLogger

	// concurency
	mu sync.Mutex

	// extra modules
	synchronizer *SynchronizeModule
	management   *ManagementModule
	recoverer    *RecoveryModule
	paxos        *PaxosModule

	grpc *grpc.Server
	replicaApi.UnimplementedReplicaServiceServer
	managementApi.ManagementServiceServer
	paxosApi.UnimplementedPaxosServiceServer
}

// region struct methods

func NewReplicaServiceServer(
	primaryAddr string,

	addr string,
	isPrimary bool,
	lockClient lockapi.LockServiceClient,
	logger seelog.LoggerInterface,
	filelogger *fl.JsonFileLogger,
	paxosFilelogger *fl.JsonFileLogger,

	grpc *grpc.Server,
) *ReplicaServiceServer {
	r := &ReplicaServiceServer{
		addr:      addr,
		isPrimary: isPrimary,
		viewId:    1,
		view:      []string{addr},

		expectedSeqNum: 0,
		nextSeqNum:     0,

		isRecovered:     false,
		executedMethods: []*replicaApi.MethodRequest{},

		lockClient:      lockClient,
		logger:          logger,
		filelogger:      filelogger,
		paxosFilelogger: paxosFilelogger,

		grpc: grpc,
	}

	r.synchronizer = NewSyncronizer(logger)
	r.recoverer = NewRecoveryModule(logger, filelogger, r.getExecutedMethods, r.addExecutedMethod, r.handleExecuteMethod, r.logToFileExecutedMethod)
	r.paxos = NewPaxosModule(r.setViewDetails, logger, paxosFilelogger)
	r.management = NewViewManager(logger, r.recoverer, r.paxos, r.getAddr, r.getViewDetails, r.getIsPrimary, r.setIsPrimary, r.setRecovered, r.setViewDetails)

	if !r.isPrimary {
		r.management.JoinToView(primaryAddr)
	} else {
		r.setRecovered(true)
	}

	r.management.StartHealthMonitoring(3)
	r.synchronizer.Start()

	return r
}

// region Getter functions

func (r *ReplicaServiceServer) getAddr() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.addr
}

func (r *ReplicaServiceServer) getViewDetails() (int64, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.viewId, r.view
}

func (r *ReplicaServiceServer) getIsPrimary() bool {
	return r.isPrimary
}

func (r *ReplicaServiceServer) getExecutedMethods() []*replicaApi.MethodRequest{
	return r.executedMethods
}

// endregion

// region Setter funcions

func (r *ReplicaServiceServer) setIsPrimary(isPrimary bool) {
	r.isPrimary = isPrimary
}

func (r *ReplicaServiceServer) setRecovered(isRecovered bool) {
	r.isRecovered = isRecovered
}

func (r *ReplicaServiceServer) setViewDetails(viewId int64, view []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	r.logger.Infof("[Replica] VIEW UPDATE: old viewId=%d → new viewId=%d", r.viewId, viewId)
	r.logger.Infof("[Replica] VIEW UPDATE: old view=%v → new view=%v", r.view, view)
	
	r.viewId = viewId
	r.view = view
}

func (r *ReplicaServiceServer) addExecutedMethod(req *replicaApi.MethodRequest) {
	r.executedMethods = append(r.executedMethods, req)
}

// endregion

// endregion

// region RPC

// region Replica

func (r *ReplicaServiceServer) GetView(ctx context.Context, req *replicaApi.GetViewRequest) (*replicaApi.GetViewResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.logger.Infof("[Replica] Received GetView request.")

	return &replicaApi.GetViewResponse{
		ViewId: r.viewId,
		View:   r.view,
	}, nil
}

func (r *ReplicaServiceServer) ExecuteMethod(
	ctx context.Context,
	req *replicaApi.ExecuteMethodRequest,
) (*replicaApi.ExecuteMethodResponse, error) {

	if r.isProxyRequest(req.Viewstamp) {
		r.logger.Infof("[Replica] Received ExecuteMethodRequest from Proxy server (viewId - %d, sequence - %d).", req.Viewstamp.ViewId, req.Viewstamp.Sequence)
		return r.handleProxyRequest(ctx, req)
	}

	r.logger.Infof("[Replica] Received ExecuteMethodRequest from Primary server (viewId - %d, sequence - %d).",  req.Viewstamp.ViewId, req.Viewstamp.Sequence)
	return r.handlePrimaryRequest(ctx, req)
}

func (r *ReplicaServiceServer) GetExecutedMethods(ctx context.Context, req *replicaApi.GetExecutedMethodsRequest) (*replicaApi.GetExecutedMethodsResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.logger.Infof("[Replica] Received GetExecutedMethods request.")

	return &replicaApi.GetExecutedMethodsResponse{
		MethodRequests: r.executedMethods,
	}, nil
}

func (r *ReplicaServiceServer) IsRecovered(ctx context.Context, req *replicaApi.IsRecoveredRequest) (*replicaApi.IsRecoveredResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.logger.Infof("[Replica] Received GetExecutedMethods request.")
	return &replicaApi.IsRecoveredResponse{
		IsRecovered: *proto.Bool(r.isRecovered),
	}, nil
}

func (r *ReplicaServiceServer) Stop(context.Context, *replicaApi.StopRequest) (*replicaApi.StopResponse, error) {
	r.logger.Infof("[Replica] Received stop request — initiating graceful shutdown...")

	r.synchronizer.Stop()
	r.management.StopHealthMonitoring()

	go func() {
		r.grpc.GracefulStop()
		r.logger.Infof("[Replica] gRPC lockapiServer stopped successfully")
	}()

	return &replicaApi.StopResponse{}, nil
}

// region RPC ExecuteMethod() help functionss

// region REQUEST ROUTING - Proxy vs Primary

func (s *ReplicaServiceServer) isProxyRequest(viewstamp *replicaApi.Viewstamp) bool {
	return viewstamp.Sequence == -1 && viewstamp.ViewId == -1
}

func (s *ReplicaServiceServer) handleProxyRequest(
	ctx context.Context,
	req *replicaApi.ExecuteMethodRequest,
) (*replicaApi.ExecuteMethodResponse, error) {
	if !s.isPrimary {
		return s.defaultResponse(), nil
	}

	replicationReq := s.buildReplicationRequest(req.MethodRequest)

	s.mu.Lock()
	resp, committed, err := s.executeReplication(ctx, replicationReq)
	s.mu.Unlock()

	if err != nil {
		return resp, err
	}

	if committed {
		if err := s.replicateToBackups(ctx, replicationReq); err != nil {
			return nil, err
		}
	}

	return resp, nil
}

func (s *ReplicaServiceServer) handlePrimaryRequest(
	ctx context.Context,
	req *replicaApi.ExecuteMethodRequest,
) (*replicaApi.ExecuteMethodResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	resp, _, err := s.executeReplication(ctx, req)
	return resp, err
}

// endregion

// region REPLICATION LOGIC - Execute and Commit

func (r *ReplicaServiceServer) executeReplication(
	ctx context.Context,
	req *replicaApi.ExecuteMethodRequest,
) (*replicaApi.ExecuteMethodResponse, bool, error) {

	if !r.isReadyForExecution() {
		return r.defaultResponse(), false, nil
	}

	if !r.isViewstampValid(req.Viewstamp) {
		return r.defaultResponse(), false, nil
	}

	if !r.isSequenceExecutable(req.Viewstamp.Sequence) {
		return r.defaultResponse(), false, nil
	}

	resp, err := r.handleExecuteMethod(ctx, req.MethodRequest)
	if err != nil {
		return resp, false, err
	}

	r.commitExecution(req.MethodRequest, req.Viewstamp.Sequence)
	return resp, true, nil
}

func (r *ReplicaServiceServer) replicateToBackups(
	ctx context.Context,
	req *replicaApi.ExecuteMethodRequest,
) error {
	synchReq := SynchronizeRequest{
		methodReq: req,
		view:      r.view,
	}
	r.synchronizer.AddMethodExecutionToQueue(synchReq)

	return nil
}

func (s *ReplicaServiceServer) commitExecution(method *replicaApi.MethodRequest, sequence int64) {
	s.addExecutedMethod(method)
	s.advanceExpectedSeq()

	s.logger.Infof(
		"[Replica] ExecuteMethod committed: method=%s seq=%d nextExpected=%d",
		method.MethodName,
		sequence,
		s.expectedSeqNum,
	)

	if err := s.logToFileExecutedMethod(method); err != nil {
		s.logger.Errorf(
			"[Replica] Failed to log executed method '%s' with parameters %v: %v",
			method.MethodName,
			method.MethodParameters,
			err,
		)
	}
}

// endregion

// region METHOD EXECUTION - Business Logic Dispatch

func (s *ReplicaServiceServer) handleExecuteMethod(
	ctx context.Context,
	methodReq *replicaApi.MethodRequest,
) (*replicaApi.ExecuteMethodResponse, error) {
	methodName := methodReq.MethodName
	methodParams := methodReq.MethodParameters

	var resp *replicaApi.ExecuteMethodResponse
	var err error

	switch methodName {
	case Acquire:
		s.logger.Infof("[Replica] Get Acquire request.")
		resp, err = s.handleAcquire(ctx, methodParams)

	case Release:
		s.logger.Infof("[Replica] Get Release request.")
		resp, err = s.handleRelease(ctx, methodParams)

	case Stop:
		s.logger.Infof("[Replica] Get Stop request.")
		resp, err = s.handleStop(ctx)

	default:
		s.logger.Infof("[Replica] Unknown method name %s.", methodName)
		resp, err = s.handleUnknownMethod()
	}

	return resp, err
}

func (s *ReplicaServiceServer) handleAcquire(
	ctx context.Context,
	methodParams []string,
) (*replicaApi.ExecuteMethodResponse, error) {
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
		return &replicaApi.ExecuteMethodResponse{IsPrimary: s.isPrimary}, nil
	}

	respStr := strconv.FormatBool(*resp.Success)
	return &replicaApi.ExecuteMethodResponse{
		IsPrimary: *proto.Bool(s.isPrimary),
		ReturnValue: &respStr,
	}, nil
}

func (s *ReplicaServiceServer) handleRelease(
	ctx context.Context,
	methodParams []string,
) (*replicaApi.ExecuteMethodResponse, error) {
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
		return &replicaApi.ExecuteMethodResponse{IsPrimary: *proto.Bool(s.isPrimary)}, nil
	}

	return &replicaApi.ExecuteMethodResponse{IsPrimary: *proto.Bool(s.isPrimary)}, nil
}

func (s *ReplicaServiceServer) handleStop(
	ctx context.Context,
) (*replicaApi.ExecuteMethodResponse, error) {
	defer s.Stop(ctx, &replicaApi.StopRequest{})

	_, err := s.lockClient.Stop(ctx, &lockapi.StopRequest{})
	if err != nil {
		s.logger.Errorf("[Replica] Error occurred while processing the %s request: %s.", Stop, err)
		return &replicaApi.ExecuteMethodResponse{IsPrimary: *proto.Bool(s.isPrimary)}, nil
	}

	return &replicaApi.ExecuteMethodResponse{IsPrimary: s.isPrimary}, nil
}

func (s *ReplicaServiceServer) handleUnknownMethod() (*replicaApi.ExecuteMethodResponse, error) {
	return &replicaApi.ExecuteMethodResponse{IsPrimary: *proto.Bool(s.isPrimary)}, nil
}

// endregion

// region VALIDATION - Pre-execution Checks

func (s *ReplicaServiceServer) isReadyForExecution() bool {
	if !s.isRecovered {
		s.logger.Warnf(
			"[Replica] ExecuteMethod rejected: replica %s not recovered",
			s.addr,
		)
		return false
	}
	return true
}

func (s *ReplicaServiceServer) isViewstampValid(vs *replicaApi.Viewstamp) bool {
	if vs.ViewId != s.viewId {
		s.logger.Warnf(
			"[Replica] ExecuteMethod rejected: viewId mismatch (got=%d expected=%d)",
			vs.ViewId,
			s.viewId,
		)
		return false
	}
	return true
}

func (s *ReplicaServiceServer) isSequenceExecutable(seq int64) bool {
	expected := s.expectedSeqNum

	if seq < expected {
		s.logger.Infof(
			"[Replica] Duplicate ExecuteMethod ignored: seq=%d expected=%d",
			seq,
			expected,
		)
		return false
	}

	if seq > expected {
		s.logger.Warnf(
			"[Replica] ExecuteMethod rejected: sequence gap (seq=%d expected=%d)",
			seq,
			expected,
		)
		return false
	}

	return true
}

// endregion

// region STATE MANAGEMENT - Sequence Numbers and Viewstamps

func (s *ReplicaServiceServer) buildReplicationRequest(
	method *replicaApi.MethodRequest,
) *replicaApi.ExecuteMethodRequest {
	vs := s.nextViewstamp()

	return &replicaApi.ExecuteMethodRequest{
		Viewstamp:     vs,
		MethodRequest: method,
	}
}

func (s *ReplicaServiceServer) nextViewstamp() *replicaApi.Viewstamp {
	vs := &replicaApi.Viewstamp{
		ViewId:   s.viewId,
		Sequence: s.nextSeqNum,
	}

	s.logger.Infof("[Replica] Created viewstamp (viewId=%d seq=%d)", vs.ViewId, vs.Sequence)
	s.advanceNextSeq()

	return vs
}

func (s *ReplicaServiceServer) advanceNextSeq() {
	s.logger.Infof("[Replica] Advancing next sequence to %d", s.nextSeqNum+1)
	s.nextSeqNum++
}

func (s *ReplicaServiceServer) advanceExpectedSeq() {
	s.logger.Infof("[Replica] Advancing expected sequence to %d", s.expectedSeqNum+1)
	s.expectedSeqNum++
}

// endregion

// region LOGGING AND UTILITIES

func (r *ReplicaServiceServer) logToFileExecutedMethod(methodReq *replicaApi.MethodRequest) error {
	timestamp := utils.GetTimeStamp()

	entry := fl.MethodLogEntry{
		Timestamp: timestamp,
		Method:    methodReq.MethodName,
		Params:    methodReq.MethodParameters,
	}

	return r.filelogger.Append(entry)
}

func (s *ReplicaServiceServer) defaultResponse() *replicaApi.ExecuteMethodResponse {
	return &replicaApi.ExecuteMethodResponse{
		IsPrimary: *proto.Bool(s.isPrimary),
	}
}

// endregion

// endregion

// endregion

// region Management

func (r *ReplicaServiceServer) Heartbeat(ctx context.Context, req *managementApi.HeartbeatRequest) (*managementApi.HeartbeatResponse, error) {
	r.logger.Infof("[Replica] Got HeartBeat from node %s.", req.Sender)
	return &managementApi.HeartbeatResponse{}, nil
}

// endregion

// region Paxos

func (r *ReplicaServiceServer) Prepare(ctx context.Context, req *paxosApi.PrepareRequest) (*paxosApi.PrepareResponse, error) {
	r.logger.Infof("[Replica] Received Prepare request.")
	return r.paxos.Prepare(req)
}

func (r *ReplicaServiceServer) Accept(ctx context.Context, req *paxosApi.AcceptRequest) (*paxosApi.AcceptResponse, error) {
	r.logger.Infof("[Replica] Received Access request.")
	return r.paxos.Accept(req)
}

func (r *ReplicaServiceServer) Decide(ctx context.Context, req *paxosApi.DecideRequest) (*paxosApi.DecideResponse, error) {
    r.logger.Infof("[Replica] Received Decide request.")

    accepted, err := r.paxos.Decide(req)
    if err != nil {
        r.logger.Errorf("[Replica] Error during Decide process: %v", err)
        return nil, err
    }

    if !accepted {
        r.logger.Infof("[Replica] Decide rejected by Paxos (old viewId)")
        return &paxosApi.DecideResponse{}, nil
    }


    r.viewId = req.ViewId
    r.view = req.View
    
    primaryAddr := r.view[0]
    isPrimary := (primaryAddr == r.addr)
    r.isPrimary = isPrimary
	if r.isPrimary {
		return &paxosApi.DecideResponse{}, nil
	}

    r.logger.Infof("[Replica] Starting recovery from primary %s (isPrimary=%v)", primaryAddr, isPrimary)
	r.setRecovered(false)
    if err := r.recoverer.RecoverFromPrimary(primaryAddr, isPrimary); err != nil {
        r.logger.Errorf("[Replica] Recovery from primary %s failed: %v", primaryAddr, err)
    }
	r.setRecovered(true)

    return &paxosApi.DecideResponse{}, nil
}

// endregion

// endregion

