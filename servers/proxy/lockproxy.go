package main

import (
	"net"
	"os"

	utils "dfs/utils"

	lockApi "dfs/proto-gen/lock"

	proxy "dfs/services/dfs/proxy"

	seelog "github.com/cihub/seelog"
	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) < 3 {
		seelog.Critical("[Main] Usage: go run <lockserver> <port>")
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

	replicaClient, _ := utils.ConnectToReplicaClient(replicaAddr)

	s := grpc.NewServer()

	srv := proxy.NewLockProxyService(s, replicaClient, replicaAddr, logger)
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