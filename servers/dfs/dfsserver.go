package main

import (
	"log"
	"net"
	"os"

	dfsapi "dfs/proto-gen/dfs"
	dfs "dfs/services/dfs"

	"google.golang.org/grpc"
)

func main() {

	args := os.Args[1:]

	port := args[0]
	extentAddr := args[1]
	lockAddr := args[2]
	
	s := grpc.NewServer()

	srv, err := dfs.NewDfsServiceServer(lockAddr, extentAddr, s)
	if err != nil {
		log.Fatalf("failed while starting DFS server: %v", err)
	}
	dfsapi.RegisterDfsServiceServer(s, srv)

	portStr := ":" + port
	lis, err := net.Listen("tcp", portStr)
    if err != nil {
        log.Fatalf("failed to listen: %v", err)
    }

    log.Printf("DFS is running on port :%s\n", port)
    if err := s.Serve(lis); err != nil {
        log.Fatalf("failed to serve: %v", err)
    }
}