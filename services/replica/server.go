package replica

import (
	lockapi "dfs/proto-gen/lock"
	managementApi "dfs/proto-gen/management"
	paxosApi "dfs/proto-gen/paxos"
	replicaApi "dfs/proto-gen/replica"

	management "dfs/services/replica/management"

	"github.com/cihub/seelog"
	"google.golang.org/grpc"
)

type ReplicaServiceServer struct {
	grpc *grpc.Server

	lockClient     lockapi.LockServiceClient
	methodExecuted []*replicaApi.MethodRequest
	isRecovered    bool

	manager *management.ViewManager

	synchronizer *Synchronizer

	logger seelog.LoggerInterface

	replicaApi.UnimplementedReplicaServiceServer
	managementApi.ManagementServiceServer
	paxosApi.UnimplementedPaxosServiceServer
}

func NewReplicaServiceServer(
	grpc *grpc.Server,
	lockClient lockapi.LockServiceClient,
	manager *management.ViewManager,
	logger seelog.LoggerInterface,

	primaryAddr string,
) *ReplicaServiceServer {
	r := &ReplicaServiceServer{
		grpc:           grpc,
		lockClient:     lockClient,
		manager:        manager,
		methodExecuted: []*replicaApi.MethodRequest{},
		isRecovered:    false,

		logger: logger,
	}

	r.synchronizer = NewSyncronizer(logger)

	if !IsPrimaryAddressSet(primaryAddr) {
		r.manager.IsPrimary = true
		r.isRecovered = true
	} else {
		r.manager.JoinView(primaryAddr)
	}

	r.synchronizer.Start()

	go r.manager.MonitorViewHealth()

	return r
}