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

	api.UnimplementedLockServer
}

func NewLockServer(grpcServer *grpc.Server) *LockServer {
    s := &LockServer{
        locked: []string{},
        grpc:   grpcServer,
    }
    s.cond = sync.NewCond(&s.mu)
    return s
}


func (s* LockServer) Acquire(ctx context.Context, req *api.AcquireRequest) (* api.AcquireReply, error) {
	s.mu.Lock() // block server so there won`t be 2 person trying to get 1 file
    defer s.mu.Unlock() // auto unlock when function ends

	// check if file has been locked already
	// if it is we block a client
	// we use "for" - if we have multiple clients that wants lock for that file that will iterate throw users
	for slices.Contains(s.locked, req.LockId) {
		s.cond.Wait()
	}

	s.locked = append(s.locked, req.LockId)
	return &api.AcquireReply{Result: true}, nil
}

func (s *LockServer) Release(ctx context.Context, req *api.ReleaseRequest) (*api.ReleaseReply, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    // шукаємо і видаляємо елемент зі слайсу
    for i, v := range s.locked {
        if v == req.LockId {
            // видаляємо req.LockId зі списку locked
            s.locked = append(s.locked[:i], s.locked[i+1:]...)
            s.cond.Broadcast() // будимо клієнтів, які чекали
            break
        }
    }

    return &api.ReleaseReply{Result: true}, nil
}

func (s *LockServer) Stop(ctx context.Context, req *api.StopRequest) (*api.StopReply, error) {
    go func() {
        // shut down server in a goroutine so we can return the response first
        s.grpc.GracefulStop()
    }()
    return &api.StopReply{Result: true}, nil
}