package main

import (
	"log"
	"net"
	"os"

	dfsapi "dfs/proto-gen/dfs"
	extentapi "dfs/proto-gen/extent"
	lockapi "dfs/proto-gen/lock"
	dfs "dfs/services/dfs"

	seelog "github.com/cihub/seelog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	args := os.Args[1:]
	if len(args) < 3 {
		log.Fatalf("usage: dfsserver <listen_addr> <extent_addr> <lock_addr>")
	}

	port := args[0]
	extentAddr := args[1]
	lockAddr := args[2]

	s := grpc.NewServer()

	lockClient := connectLockClient(lockAddr)
	if lockClient == nil {
		log.Fatalf("[DfsServer] failed to connect to lock server at %s", lockAddr)
	}

	extentClient := connectExtentClient(extentAddr)
	if extentClient == nil {
		log.Fatalf("[DfsServer] failed to connect to extent server at %s", extentAddr)
	}

	dfsClient := dfs.NewDfsClient(port)

	logger, _ := seelog.LoggerFromConfigAsFile("configs/seelog-dfs.xml")

	srv, err := dfs.NewDfsServiceServer(lockClient, extentClient, dfsClient, s, logger)
	if err != nil {
		log.Fatalf("failed while starting DFS server: %v", err)
	}

	dfsapi.RegisterDfsServiceServer(s, srv)

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", port, err)
	}

	log.Printf("DFS is running on %s\n", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func connectLockClient(lockAddr string) lockapi.LockServiceClient {
	conn, err := grpc.NewClient(lockAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("failed to dial lock server at %s: %v", lockAddr, err)
		return nil
	}
	return lockapi.NewLockServiceClient(conn)
}

func connectExtentClient(extentAddr string) extentapi.ExtentServiceClient {
	conn, err := grpc.NewClient(extentAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("failed to dial extent server at %s: %v", extentAddr, err)
		return nil
	}
	return extentapi.NewExtentServiceClient(conn)
}
