package main

import (
	"log"
	"net"

	"os"
	lockapi "dfs/proto-gen/lock"
	lock "dfs/services/lock"
	"google.golang.org/grpc"
)

func main() {
	port := os.Args[1]
	
	s := grpc.NewServer()

	srv := lock.NewLockServer(s)
	lockapi.RegisterLockServiceServer(s, srv)

	portStr := ":" + port
	lis, err := net.Listen("tcp", portStr)
    if err != nil {
        log.Fatalf("failed to listen: %v", err)
    }

    log.Printf("Lock is running on port :%s\n", port)
    if err := s.Serve(lis); err != nil {
        log.Fatalf("failed to serve: %v", err)
    }
}