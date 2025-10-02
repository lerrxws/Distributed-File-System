package dfs

import (
	"context"
	api "dfs/proto-gen/dfs"
	extent "dfs/proto-gen/extent"
	lock "dfs/proto-gen/lock"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
		return nil, fmt.Errorf("failed to create Lock Service client: %v", err)
	}

	lockClient := lock.NewLockServiceClient(lockConn)

	extentConn, err := grpc.NewClient(extentAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create Extent Service client: %v", err)
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
    if err != nil || !lockResp.Success {
        return fmt.Errorf("failed to acquire lock for %s", lockId)
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
	if strings.HasSuffix(req.DirectoryName, "/") {
		return &api.DirResponse{Success: false}, fmt.Errorf("invalid directory path: %s, path must end with '/'", req.DirectoryName)
	}

	if req.DirectoryName == "" {
		return &api.DirResponse{Success: false}, fmt.Errorf("directory name is required")
	}

	err := acquireLock(ctx, s.lockClient, req.DirectoryName)
	if err != nil {
		return &api.DirResponse{Success: false}, fmt.Errorf("failed to acquire lock for directory %s", req.DirectoryName)
	}
	defer releaseLock(ctx, s.lockClient, req.DirectoryName)


	resp, err := s.extentClient.Get(ctx, &extent.GetRequest{
		FileName: req.DirectoryName,
	})
	if err != nil || (resp == &extent.GetResponse{}) {
		return &api.DirResponse{Success: false}, fmt.Errorf("unable to process a directory %s", req.DirectoryName)
	}

	fileDataString := string(resp.FileData)
	fileDataList := strings.Split(fileDataString, "\n")
	return &api.DirResponse{Success: true, DirList: fileDataList}, nil
}

func (s *DfsServiceServer) Mkdir(ctx context.Context, req *api.MkdirRequest) (*api.MkdirResponse, error) {
	if req.DirectoryName == "" {
		return &api.MkdirResponse{Success: false}, fmt.Errorf("directory name is required")
	}

	if !strings.HasSuffix(req.DirectoryName, "/") {
		return &api.MkdirResponse{Success: false}, fmt.Errorf("invalid directory path: %s, path must end with '/'", req.DirectoryName)
	}

	err := acquireLock(ctx, s.lockClient, req.DirectoryName)
	if err != nil {
		return &api.MkdirResponse{Success: false}, fmt.Errorf("failed to acquire lock for directory %s", req.DirectoryName)
	}
	defer releaseLock(ctx, s.lockClient, req.DirectoryName)

	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{
		FileName: req.DirectoryName,
		FileData: []byte{},
	})
	if err != nil || !resp.Success {
		return &api.MkdirResponse{Success: false}, fmt.Errorf("unable to create directory %s", req.DirectoryName)
	}

	return &api.MkdirResponse{Success: true}, nil
}
func (s *DfsServiceServer) Rmdir(ctx context.Context, req *api.RmdirRequest) (*api.RmdirResponse, error) {
	if req.DirectoryName == "" {
		return &api.RmdirResponse{Success: false}, fmt.Errorf("directory name is required")
	}

	if !strings.HasSuffix(req.DirectoryName, "/") {
		return &api.RmdirResponse{Success: false}, fmt.Errorf("invalid directory path: %s, path must end with '/'", req.DirectoryName)
	}

	err := acquireLock(ctx, s.lockClient, req.DirectoryName)
	if err != nil {
		return &api.RmdirResponse{Success: false}, fmt.Errorf("failed to acquire lock for directory %s", req.DirectoryName)
	}
	defer releaseLock(ctx, s.lockClient, req.DirectoryName)

	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{FileName: req.DirectoryName})
	if err != nil || !resp.Success {
    	return &api.RmdirResponse{Success: false}, fmt.Errorf("unable to remove directory %s", req.DirectoryName)
	}


	return &api.RmdirResponse{Success: true}, nil
}

func (s *DfsServiceServer) Put(ctx context.Context, req *api.PutRequest) (*api.PutResponse, error) {
	if req.FileName == "" {
		return &api.PutResponse{Success: false}, fmt.Errorf("file name is required")
	}

	if strings.HasSuffix(req.FileName, "/") {
		return &api.PutResponse{Success: false}, fmt.Errorf("invalid directory path: %s, path can not end with '/'", req.FileName)
	}

	err := acquireLock(ctx, s.lockClient, req.FileName)
	if err != nil {
		return &api.PutResponse{Success: false}, fmt.Errorf("failed to acquire lock for file %s", req.FileName)
	}
	defer releaseLock(ctx, s.lockClient, req.FileName)

	if req.FileData == nil {
    	return &api.PutResponse{Success: false}, fmt.Errorf("file data cannot be nil for the operation. The file data must be provided to perform the operation")
	}

	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{FileName: req.FileName, FileData: req.FileData})
	if err != nil || !resp.Success {
		return &api.PutResponse{Success: false}, fmt.Errorf("unable to create a file %s", req.FileName)
	}

	return &api.PutResponse{Success: true}, nil
}

func (s *DfsServiceServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error){
	if req.FileName == "" {
		return &api.GetResponse{}, fmt.Errorf("file name is required")
	}

	if strings.HasSuffix(req.FileName, "/") {
		return &api.GetResponse{}, fmt.Errorf("invalid file path: %s, path can not end with '/'", req.FileName)
	}

	err := acquireLock(ctx, s.lockClient, req.FileName)
	if err != nil {
		return &api.GetResponse{}, fmt.Errorf("failed to acquire lock for file %s", req.FileName)
	}
	defer releaseLock(ctx, s.lockClient, req.FileName)

	resp, err := s.extentClient.Get(ctx, &extent.GetRequest{FileName: req.FileName})
	if (resp == &extent.GetResponse{}) || err != nil {
    	return &api.GetResponse{}, fmt.Errorf("unable to process file %s", req.FileName)
	}

	return &api.GetResponse{FileData: resp.FileData}, nil

}

func (s *DfsServiceServer) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	if req.FileName == "" {
		return &api.DeleteResponse{Success: false}, fmt.Errorf("file name is required")
	}

	if strings.HasSuffix(req.FileName, "/") {
		return &api.DeleteResponse{Success: false}, fmt.Errorf("invalid file path: %s, path can not end with '/'", req.FileName)
	}

	err := acquireLock(ctx, s.lockClient, req.FileName)
	if err != nil {
		return &api.DeleteResponse{Success: false}, fmt.Errorf("failed to acquire lock for directory %s", req.FileName)
	}
	defer releaseLock(ctx, s.lockClient, req.FileName)

	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{FileName: req.FileName})
	if err != nil || !resp.Success {
		return &api.DeleteResponse{Success: false}, fmt.Errorf("unable to process file %s", req.FileName)
	}

	return &api.DeleteResponse{Success: true}, nil
}
