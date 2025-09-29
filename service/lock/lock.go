package lock

import (
	"context"
	"sync"
	"slices"
	api "dfs/proto-gen/lock"

	"google.golang.org/grpc"
)

// GRPCServer
type LockServer struct {
	mu    sync.Mutex
    cond  *sync.Cond        
    locked []string
	grpc  *grpc.Server

	api.UnimplementedLockServiceServer
}

func NewLockServer(grpcServer *grpc.Server) *LockServer {
    s := &LockServer{
        locked: []string{},
        grpc:   grpcServer,
    }
    s.cond = sync.NewCond(&s.mu)
    return s
}


func (s* LockServer) Acquire(ctx context.Context, req *api.AcquireRequest) (* api.AcquireResponse, error) {
	s.mu.Lock() // block server so there won`t be 2 person trying to get 1 file
    defer s.mu.Unlock() // auto unlock when function ends

	// check if file has been locked already
	// if it is we block a client
	// we use "for" - if we have multiple clients that wants lock for that file that will iterate throw users
	for slices.Contains(s.locked, req.LockId) {
		s.cond.Wait()
	}

	s.locked = append(s.locked, req.LockId)
	return &api.AcquireResponse{Success: true}, nil
}

func (s *LockServer) Release(ctx context.Context, req *api.ReleaseRequest) (*api.ReleaseResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

	if !slices.Contains(s.locked, req.LockId) {
		return &api.ReleaseResponse{}, nil
	}

    for i, v := range s.locked {
        if v == req.LockId {
            s.locked = append(s.locked[:i], s.locked[i+1:]...)
            s.cond.Broadcast()
            break
        }
    }

    return &api.ReleaseResponse{}, nil
}

func (s *LockServer) Stop(ctx context.Context, req *api.StopRequest) (*api.StopResponse, error) {
    go func() {
        // shut down server in a goroutine so we can return the response first
        s.grpc.GracefulStop()
    }()
    return &api.StopResponse{}, nil
}