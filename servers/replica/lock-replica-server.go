package main

import (
	"net"
	"os"

	lockreplica "dfs/services/lock/replica"
	lockApi "dfs/proto-gen/lock"
	replicaApi "dfs/proto-gen/replica"

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

	logger, err := seelog.LoggerFromConfigAsFile("configs/logs/lock/seelog-lockreplica.xml")
	if err != nil {
		seelog.Criticalf("[Main] Failed to load seelog config: %v", err)
		os.Exit(1)
	}
	defer seelog.Flush()

	logger.Infof("[Main] Starting LockProxyServer initialization...")

	lockClient := connectLockClient(replicaAddr, logger)

	s := grpc.NewServer()

	addr := "127.0.0.1:" + port
	srv := lockreplica.NewReplicaServiceServer(s, lockClient, addr, logger)
	replicaApi.RegisterReplicaServiceServer(s, srv)
	

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


func connectLockClient(lockAddr string, logger seelog.LoggerInterface) lockApi.LockServiceClient{
	conn, err := grpc.NewClient(lockAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Errorf("[Main] Failed to connect to LockReplica server at %s: %v", lockAddr, err)
	}
	logger.Infof("[Main] Connected to LockReplica server at %s", lockAddr)
	return lockApi.NewLockServiceClient(conn)
}