package main

import (
	"net"
	"os"

	lockapi "dfs/proto-gen/lock"
	lock "dfs/services/lock"

	seelog "github.com/cihub/seelog"
	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) < 2 {
		seelog.Critical("[Main] Usage: go run lockserver.go <port>")
		os.Exit(1)
	}
	port := os.Args[1]

	logger, err := seelog.LoggerFromConfigAsFile("configs/logs/seelog-lock.xml")
	if err != nil {
		seelog.Criticalf("[Main] Failed to load seelog config: %v", err)
		os.Exit(1)
	}
	defer seelog.Flush()

	logger.Infof("[Main] Starting LockServer initialization...")

	s := grpc.NewServer()

	revoker := lock.NewRevokeTask(logger)
	retrier := lock.NewRetryTask(logger)
	 
	srv := lock.NewLockServer(s, revoker, retrier, logger)
	lockapi.RegisterLockServiceServer(s, srv)

	portStr := ":" + port
	lis, err := net.Listen("tcp", portStr)
	if err != nil {
		logger.Criticalf("[Main] Failed to listen on %s: %v", portStr, err)
		os.Exit(1)
	}

	logger.Infof("[Main] LockServer is running on port %s", portStr)

	if err := s.Serve(lis); err != nil {
		logger.Criticalf("[Main] Failed to serve gRPC: %v", err)
		os.Exit(1)
	}

	logger.Infof("[Main] LockServer shutdown completed.")
}
