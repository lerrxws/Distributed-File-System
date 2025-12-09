package main

import (
	"net"
	"os"

	lockApi "dfs/proto-gen/lock"
	replicaApi "dfs/proto-gen/replica"

	lockproxy "dfs/services/lock/proxy"

	seelog "github.com/cihub/seelog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 3 {
		seelog.Critical("[Main] Usage: go run lockserver.go <port>")
		os.Exit(1)
	}

	replicaAddr := os.Args[1]
	port := os.Args[2]

	logger, err := seelog.LoggerFromConfigAsFile("configs/logs/lock/seelog-lockproxy.xml")
	if err != nil {
		seelog.Criticalf("[Main] Failed to load seelog config: %v", err)
		os.Exit(1)
	}
	defer seelog.Flush()

	logger.Infof("[Main] Starting LockProxyServer initialization...")

	replicaClient := connectReplicaClient(replicaAddr, logger)

	s := grpc.NewServer()

	srv := lockproxy.NewLockProxyService(s, replicaClient)
	lockApi.RegisterLockServiceServer(s, srv)

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


func connectReplicaClient(replicaAddr string, logger seelog.LoggerInterface) replicaApi.ReplicaServiceClient{
	conn, err := grpc.NewClient(replicaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Errorf("[Main] Failed to connect to LockReplica server at %s: %v", replicaAddr, err)
	}
	logger.Infof("[Main] Connected to LockReplica server at %s", replicaAddr)
	return replicaApi.NewReplicaServiceClient(conn)
}