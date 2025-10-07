package main

import (
	"log"
	"net"
	"os"

	extentapi "dfs/proto-gen/extent"
	extent "dfs/services/extent"

	"google.golang.org/grpc"
)

func main() {

	args := os.Args[1:]

	port := args[0]
	rootpath := args[1]
	
	s := grpc.NewServer()

	srv := extent.NewExtentServiceServer(rootpath, s)
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