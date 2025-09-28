package main

import (
	"log"
	"net"

	// "sync"

	api "dfs/proto-gen/lock"
	// api "dfs/proto-gen/extent"
	lock "dfs/service/lock"
	// extent "dfs/service/extent"

	"google.golang.org/grpc"
)

func main() {
	s := grpc.NewServer()

	srv := lock.NewLockServer(s)
	// srv := extent.NewExtentServer("C:/Users/lerrxwsb/Desktop/tuke/DS/dfs/", s)


	api.RegisterLockServer(s, srv)
	// api.RegisterExtentServer(s, srv)
	lis, err := net.Listen("tcp", ":8080")
    if err != nil {
        log.Fatalf("failed to listen: %v", err)
    }

    log.Println("LockServer is running on port :8080")
    if err := s.Serve(lis); err != nil {
        log.Fatalf("failed to serve: %v", err)
    }
}