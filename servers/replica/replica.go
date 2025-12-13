package main

import (
	"net"
	"os"

	utils "dfs/utils"

	replica "dfs/services/replica"
	management "dfs/services/replica/management"

	managementApi "dfs/proto-gen/management"
	paxosApi "dfs/proto-gen/paxos"
	replicaApi "dfs/proto-gen/replica"

	seelog "github.com/cihub/seelog"
	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) < 2 {
		seelog.Critical("[Main] Usage: go run lockserver.go <port>")
		os.Exit(1)
	}

	lockClientAddr := os.Args[1]
	port := os.Args[2]

	primaryAddr := ""
	if len(os.Args) == 4{
		primaryAddr = os.Args[3]
	}

	logger, err := utils.CreateLoggerWithPortNumber(port)

	if err != nil {
		seelog.Criticalf("[Main] Failed to load seelog config: %v", err)
		os.Exit(1)
	}
	defer seelog.Flush()

	s := grpc.NewServer()

	addr := "127.0.0.1:" + port

	heartbeatIntervalInSeconds := 5
	manager := management.NewViewManager(addr, heartbeatIntervalInSeconds, logger)
	lockClient := utils.ConnectLockClient(lockClientAddr, logger)

	srv := replica.NewReplicaServiceServer(s, lockClient, manager, logger, primaryAddr)

	replicaApi.RegisterReplicaServiceServer(s, srv)
	managementApi.RegisterManagementServiceServer(s, srv)
	paxosApi.RegisterPaxosServiceServer(s, srv)
	

	portStr := ":" + port
	lis, err := net.Listen("tcp", portStr)
	if err != nil {
		logger.Criticalf("[Main] Failed to listen on %s: %v", portStr, err)
		os.Exit(1)
	}

	logger.Infof("[Main] LockProxyServer is running on port %s", portStr)

	if err := s.Serve(lis); err != nil {
		logger.Criticalf("[Main] Failed to serve gRPC: %v", err)
		os.Exit(1)
	}

	logger.Infof("[Main] LockProxyServer shutdown completed.")
}