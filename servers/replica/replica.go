package main

import (
	"fmt"
	"net"
	"os"
	"strconv"

	utils "dfs/utils"

	replica "dfs/services/replica"

	managementApi "dfs/proto-gen/management"
	paxosApi "dfs/proto-gen/paxos"
	replicaApi "dfs/proto-gen/replica"

	fl "dfs/utils/filelogger"

	seelog "github.com/cihub/seelog"
	"google.golang.org/grpc"
)

func main() {
	// parse input 
	lockClientAddr, port, isPrimary, primaryAddr := parseInput()
	addr := getNodeAddr(port)

	// init logger
	logger, err := utils.CreateLoggerWithPortNumber(port)
	if err != nil {
		panic("failed to load seelog config.")
	}
	defer seelog.Flush()

	// init filelogger
	logFile := fmt.Sprintf("%s.json", port)
	storagePath := "storage/replica/"

	executionLogger, err := createJsonFileLogger(storagePath+"execution/", logFile, logger)
	if err != nil {
		os.Exit(1)
	}

	paxosLogger, err := createJsonFileLogger(storagePath+"paxos/", logFile, logger)
	if err != nil {
		os.Exit(1)
	}

	// connect to lock server
	lockClient, err := utils.ConnectLockClient(lockClientAddr, logger)
	if err != nil {
		// panic("failed to cinnect to lock server.")
	}

	s := grpc.NewServer()

	srv := replica.NewReplicaServiceServer(primaryAddr, addr, isPrimary, lockClient, logger, executionLogger, paxosLogger, s)

	replicaApi.RegisterReplicaServiceServer(s, srv)
	managementApi.RegisterManagementServiceServer(s, srv)
	paxosApi.RegisterPaxosServiceServer(s, srv)
	

	portStr := ":" + port
	lis, err := net.Listen("tcp", portStr)
	if err != nil {
		logger.Criticalf("[Main] Failed to listen on %s: %v", portStr, err)
		os.Exit(1)
	}

	logger.Infof("[Main] Replica is running on port %s", portStr)

	if err := s.Serve(lis); err != nil {
		logger.Criticalf("[Main] Failed to serve gRPC: %v", err)
		os.Exit(1)
	}

	logger.Infof("[Main] Replica shutdown completed.")
}

func parseInput() (lockClientAddr string, addr string, isPrimary bool, primaryAddr string) {
	if len(os.Args) < 4 {
		panic("usage: <lockClientAddr> <port> <isPrimary>")
	}

	lockClientAddr = os.Args[1]
	addr = os.Args[2]
	var err error
	isPrimary, err = strconv.ParseBool(os.Args[3])
	if err != nil {
		panic(fmt.Sprintf("invalid boolean value for isPrimary: %v", os.Args[2]))
	}

	primaryAddr = ""
	if !isPrimary {
		if len(os.Args) < 5 {
			panic("usage: <lockClientAddr> <port> <false> <primaryAddr>")
		}
		primaryAddr = os.Args[4]
	}

	return lockClientAddr, addr, isPrimary, primaryAddr
}

func getNodeAddr(port string) string {
	return "127.0.0.1:" + port
}

func createJsonFileLogger(logPath, logFile string, logger seelog.LoggerInterface) (*fl.JsonFileLogger, error) {
	logFilePath := logPath + logFile
	loggerInstance, err := fl.NewJsonFileLogger(logFilePath)
	if err != nil {
		logger.Criticalf(
			"[Main] Failed to initialize logger at path '%s': %v",
			logFilePath,
			err,
		)
		return nil, fmt.Errorf("failed to initialize logger at path '%s': %v", logFilePath, err)
	}
	return loggerInstance, nil
}