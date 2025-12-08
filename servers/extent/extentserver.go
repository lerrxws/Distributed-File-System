package main

import (
	"log"
	"net"
	"os"

	extentapi "dfs/proto-gen/extent"
	extent "dfs/services/extent"

	seelog "github.com/cihub/seelog"
	"google.golang.org/grpc"
)

func main() {

	args := os.Args[1:]

	port := args[0]
	rootpath := args[1]
	
	logger := initLogger("configs/logs/seelog-extent.xml")

	s := grpc.NewServer()

	srv := extent.NewExtentServiceServer(rootpath, s, logger)
	extentapi.RegisterExtentServiceServer(s, srv)

	portStr := ":" + port
	lis, err := net.Listen("tcp", portStr)
    if err != nil {
        log.Fatalf("failed to listen: %v", err)
    }

    log.Printf("Extent is running on port :%s\n", port)
    if err := s.Serve(lis); err != nil {
        log.Fatalf("failed to serve: %v", err)
    }
}

func initLogger(configPath string) seelog.LoggerInterface {
	logger, err := seelog.LoggerFromConfigAsFile(configPath)
	if err != nil {
		log.Fatalf("Failed to initialize logger from %s: %v", configPath, err)
	}
	return logger
}