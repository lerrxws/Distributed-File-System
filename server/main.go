package main

import (
	"log"
	"net"
	// "sync"

	api "dfs/proto-gen/lock"
	lock "dfs/service/lock"

	"google.golang.org/grpc"
)

func main() {
	s := grpc.NewServer()

	srv := lock.NewLockServer(s)

	api.RegisterLockServer(s, srv)
	lis, err := net.Listen("tcp", ":8080")
    if err != nil {
        log.Fatalf("failed to listen: %v", err)
    }

    log.Println("LockServer is running on port :8080")
    if err := s.Serve(lis); err != nil {
        log.Fatalf("failed to serve: %v", err)
    }
}