package main

import (
	"log"
	"net"
	"os"

	dfsapi "dfs/proto-gen/dfs"
	lcapi "dfs/proto-gen/lockcache"
	extentapi "dfs/proto-gen/extent"
	lockapi "dfs/proto-gen/lock"

	dfs "dfs/services/dfs"
	lockcache "dfs/services/lockcache"

	seelog "github.com/cihub/seelog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 4 {
		log.Fatalf("Usage: dfsserver <listen_port> <extent_addr> <lock_addr>")
	}

	port := os.Args[1]
	extentAddr := os.Args[2]
	lockAddr := os.Args[3]

	logger := initLogger("configs/seelog-dfs.xml")
	defer logger.Flush()

	logger.Infof("[Main] Starting DFS server on port %s", port)

	grpcServer := grpc.NewServer()

	lockClient := connectLockClient(lockAddr)
	extentClient := connectExtentClient(extentAddr)
	dfsClient := dfs.NewDfsClient(port)

	cacheManager := lockcache.NewCacheManager(lockClient, logger)
	releaser := lockcache.NewReleaser(cacheManager, logger)
	releaser.Start()


	dfsService, err := dfs.NewDfsServiceServer(lockClient, cacheManager, extentClient, dfsClient, grpcServer, logger)
	if err != nil {
		logger.Criticalf("[Main] Failed to initialize DFS service: %v", err)
		os.Exit(1)
	}

	lockCacheService := lockcache.NewLockCacheService(grpcServer, cacheManager, releaser, logger)

	dfsapi.RegisterDfsServiceServer(grpcServer, dfsService)
	lcapi.RegisterLockCacheServiceServer(grpcServer, lockCacheService)

	startGrpcServer(grpcServer, port, logger)
}

func initLogger(configPath string) seelog.LoggerInterface {
	logger, err := seelog.LoggerFromConfigAsFile(configPath)
	if err != nil {
		log.Fatalf("Failed to initialize logger from %s: %v", configPath, err)
	}
	return logger
}

func connectLockClient(addr string) lockapi.LockServiceClient {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[Main] Failed to connect to Lock server at %s: %v", addr, err)
	}
	log.Printf("[Main] Connected to Lock server at %s", addr)
	return lockapi.NewLockServiceClient(conn)
}

func connectExtentClient(addr string) extentapi.ExtentServiceClient {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[Main] Failed to connect to Extent server at %s: %v", addr, err)
	}
	log.Printf("[Main] Connected to Extent server at %s", addr)
	return extentapi.NewExtentServiceClient(conn)
}

func startGrpcServer(server *grpc.Server, port string, logger seelog.LoggerInterface) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Criticalf("Failed to listen on %s: %v", port, err)
		os.Exit(1)
	}

	logger.Infof("[Main] DFS is running on port %s", port)
	if err := server.Serve(listener); err != nil {
		logger.Criticalf("gRPC serve failed: %v", err)
		os.Exit(1)
	}
}
