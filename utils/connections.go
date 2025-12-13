package utils

import (
	lockApi "dfs/proto-gen/lock"
	managementApi "dfs/proto-gen/management"
	replicaApi "dfs/proto-gen/replica"

	"github.com/cihub/seelog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func ConnectToReplicaClient(addr string) (replicaApi.ReplicaServiceClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return replicaApi.NewReplicaServiceClient(conn), nil
}

func ConnectToManagementClient(addr string) (managementApi.ManagementServiceClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return managementApi.NewManagementServiceClient(conn), nil
}

func ConnectLockClient(lockAddr string, logger seelog.LoggerInterface) lockApi.LockServiceClient{
	conn, err := grpc.NewClient(lockAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Errorf("[Main] Failed to connect to LockReplica server at %s: %v", lockAddr, err)
	}
	logger.Infof("[Main] Connected to LockReplica server at %s", lockAddr)
	return lockApi.NewLockServiceClient(conn)
}