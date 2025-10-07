package dfs

import (
	"context"
	api "dfs/proto-gen/dfs"
	extent "dfs/proto-gen/extent"
	lock "dfs/proto-gen/lock"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type DfsServiceServer struct {
	lockClient   lock.LockServiceClient
	extentClient extent.ExtentServiceClient
	grpc         *grpc.Server

	api.UnimplementedDfsServiceServer
}

func NewDfsServiceServer(lockAddr, extentAddr string, grpcServer *grpc.Server) (*DfsServiceServer, error) {
	lockConn, err := grpc.NewClient(lockAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil
	}

	lockClient := lock.NewLockServiceClient(lockConn)

	extentConn, err := grpc.NewClient(extentAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil
	}

	extentClient := extent.NewExtentServiceClient(extentConn)

	return &DfsServiceServer{
		lockClient:   lockClient,
		extentClient: extentClient,
		grpc:         grpcServer,
	}, nil
}

func acquireLock(ctx context.Context, lockClient lock.LockServiceClient, lockId string) error {
	lockResp, err := lockClient.Acquire(ctx, &lock.AcquireRequest{LockId: lockId, OwnerId: "user", Sequence: 1})
	if err != nil || !(*lockResp.Success) {
		return nil
	}
	return nil
}

func releaseLock(ctx context.Context, lockClient lock.LockServiceClient, lockId string) {
	lockClient.Release(ctx, &lock.ReleaseRequest{LockId: lockId, OwnerId: "user"})
}

func (s *DfsServiceServer) Stop(ctx context.Context, req *api.StopRequest) (*api.StopResponse, error) {
	go func() {
		s.grpc.GracefulStop()
	}()
	return &api.StopResponse{}, nil
}

func (s *DfsServiceServer) Dir(ctx context.Context, req *api.DirRequest) (*api.DirResponse, error) {
	if !strings.HasSuffix(req.DirectoryName, "/") {
		return &api.DirResponse{Success: proto.Bool(false)}, nil
	}

	if req.DirectoryName == "" {
		return &api.DirResponse{Success: proto.Bool(false)}, nil
	}

	err := acquireLock(ctx, s.lockClient, req.DirectoryName)
	if err != nil {
		return &api.DirResponse{Success: proto.Bool(false)}, nil
	}
	defer releaseLock(ctx, s.lockClient, req.DirectoryName)

	resp, err := s.extentClient.Get(ctx, &extent.GetRequest{
		FileName: req.DirectoryName,
	})
	if err != nil {
		return &api.DirResponse{Success: proto.Bool(false)}, nil
	}
	if resp == nil {
		return &api.DirResponse{Success: proto.Bool(false)}, nil
	}

	fileDataString := string(resp.FileData)
	fileDataList := strings.Split(fileDataString, "\n")
	return &api.DirResponse{Success: proto.Bool(true), DirList: fileDataList}, nil
}


func (s *DfsServiceServer) Mkdir(ctx context.Context, req *api.MkdirRequest) (*api.MkdirResponse, error) {
	if req.DirectoryName == "" {
		return &api.MkdirResponse{Success: proto.Bool(false)}, nil
	}

	if !strings.HasSuffix(req.DirectoryName, "/") {
		return &api.MkdirResponse{Success: proto.Bool(false)}, nil
	}

	err := acquireLock(ctx, s.lockClient, req.DirectoryName)
	if err != nil {
		return &api.MkdirResponse{Success: proto.Bool(false)}, nil
	}
	defer releaseLock(ctx, s.lockClient, req.DirectoryName)

	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{
		FileName: req.DirectoryName,
		FileData: []byte{},
	})
	if err != nil || !(*resp.Success) {
		return &api.MkdirResponse{Success: proto.Bool(false)}, nil
	}

	return &api.MkdirResponse{Success: proto.Bool(true)}, nil
}
func (s *DfsServiceServer) Rmdir(ctx context.Context, req *api.RmdirRequest) (*api.RmdirResponse, error) {
	if req.DirectoryName == "" {
		return &api.RmdirResponse{Success: proto.Bool(false)}, nil
	}

	if !strings.HasSuffix(req.DirectoryName, "/") {
		return &api.RmdirResponse{Success: proto.Bool(false)}, nil
	}

	err := acquireLock(ctx, s.lockClient, req.DirectoryName)
	if err != nil {
		return &api.RmdirResponse{Success: proto.Bool(false)}, nil
	}
	defer releaseLock(ctx, s.lockClient, req.DirectoryName)

	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{FileName: req.DirectoryName})
	if err != nil || !(*resp.Success) {
		return &api.RmdirResponse{Success: proto.Bool(false)}, nil
	}

	return &api.RmdirResponse{Success: proto.Bool(true)}, nil
}

func (s *DfsServiceServer) Put(ctx context.Context, req *api.PutRequest) (*api.PutResponse, error) {
	if req.FileName == "" {
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	if strings.HasSuffix(req.FileName, "/") {
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	err := acquireLock(ctx, s.lockClient, req.FileName)
	if err != nil {
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}
	defer releaseLock(ctx, s.lockClient, req.FileName)

	if req.FileData == nil {
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{FileName: req.FileName, FileData: req.FileData})
	if err != nil || !(*resp.Success) {
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	return &api.PutResponse{Success: proto.Bool(true)}, nil
}

func (s *DfsServiceServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	if req.FileName == "" {
		return &api.GetResponse{}, nil
	}

	if strings.HasSuffix(req.FileName, "/") {
		return &api.GetResponse{}, nil
	}

	err := acquireLock(ctx, s.lockClient, req.FileName)
	if err != nil {
		return &api.GetResponse{}, nil
	}
	defer releaseLock(ctx, s.lockClient, req.FileName)

	resp, err := s.extentClient.Get(ctx, &extent.GetRequest{FileName: req.FileName})
	if (resp == &extent.GetResponse{}) || err != nil {
		return &api.GetResponse{}, nil
	}

	return &api.GetResponse{FileData: resp.FileData}, nil

}

func (s *DfsServiceServer) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	if req.FileName == "" {
		return &api.DeleteResponse{Success: proto.Bool(false)}, nil
	}

	if strings.HasSuffix(req.FileName, "/") {
		return &api.DeleteResponse{Success: proto.Bool(false)}, nil
	}

	err := acquireLock(ctx, s.lockClient, req.FileName)
	if err != nil {
		return &api.DeleteResponse{Success: proto.Bool(false)}, nil
	}
	defer releaseLock(ctx, s.lockClient, req.FileName)

	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{FileName: req.FileName})
	if err != nil || !(*resp.Success) {
		return &api.DeleteResponse{Success: proto.Bool(false)}, nil
	}

	return &api.DeleteResponse{Success: proto.Bool(true)}, nil
}
