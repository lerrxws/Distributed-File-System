package dfs

import (
	"context"
	"fmt"
	"strings"

	api "dfs/proto-gen/dfs"
	extent "dfs/proto-gen/extent"
	lock "dfs/proto-gen/lock"

	lockcache "dfs/services/lockcache"

	seelog "github.com/cihub/seelog"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type DfsServiceServer struct {
	lockClient   lock.LockServiceClient
	cacheManager *lockcache.CacheManager
	extentClient extent.ExtentServiceClient
	dfsClient    *DfsClient
	grpc         *grpc.Server

	logger seelog.LoggerInterface

	api.UnimplementedDfsServiceServer
}

func NewDfsServiceServer(lockClient lock.LockServiceClient, cacheManager *lockcache.CacheManager,
						 extentClient extent.ExtentServiceClient,
						dfsClient *DfsClient, grpcServer *grpc.Server, logger seelog.LoggerInterface) (*DfsServiceServer, error) {
	return &DfsServiceServer{
		lockClient:   lockClient,
		cacheManager: cacheManager,

		extentClient: extentClient,
		dfsClient:    dfsClient,
		grpc:         grpcServer,
		logger:       logger,
	}, nil
}

// region public method

func (s *DfsServiceServer) Stop(ctx context.Context, req *api.StopRequest) (*api.StopResponse, error) {
	s.logger.Infof("[DFS][Stop] Stopping gRPC server...")
	go func() {
		s.grpc.GracefulStop()
	}()
	s.logger.Infof("[DFS][Stop] Server stopped")
	return &api.StopResponse{}, nil
}

func (s *DfsServiceServer) Dir(ctx context.Context, req *api.DirRequest) (*api.DirResponse, error) {
	isCorrect, msg := s.validateName(req.DirectoryName, true)
	if !isCorrect {
		s.logger.Errorf("[DFS][Dir] Validation failed for directory %s: %s", req.DirectoryName, msg)
		return &api.DirResponse{}, nil 
	}

	err := s.acquireLock(ctx, req.DirectoryName)
	if err != nil {
		return &api.DirResponse{Success: proto.Bool(false)}, nil
	}
	defer s.releaseLock(ctx, req.DirectoryName)

	s.logger.Infof("[DFS][Dir] Fetching directory listing from extent service: %s", req.DirectoryName)
	resp, err := s.extentClient.Get(ctx, &extent.GetRequest{FileName: req.DirectoryName})
	if err != nil {
		s.logger.Errorf("[DFS][Dir] Extent service Get request failed for directory: %s, error: %v", req.DirectoryName, err)
		return &api.DirResponse{Success: proto.Bool(false)}, nil
	}
	if resp == nil {
		s.logger.Warnf("[DFS][Dir] Extent service returned nil response for directory: %s", req.DirectoryName)
		return &api.DirResponse{Success: proto.Bool(false)}, nil
	}

	fileDataString := string(resp.FileData)
	fileDataList := strings.Split(fileDataString, "\n")
	s.logger.Infof("[DFS][Dir] Directory listing retrieved successfully for %s: %d entries", req.DirectoryName, len(fileDataList))

	return &api.DirResponse{Success: proto.Bool(true), DirList: fileDataList}, nil
}

func (s *DfsServiceServer) Mkdir(ctx context.Context, req *api.MkdirRequest) (*api.MkdirResponse, error) {
	isCorrect, msg := s.validateName(req.DirectoryName, true)
	if !isCorrect {
		s.logger.Errorf("[DFS][Mkdir] Validation failed for directory %s: %s", req.DirectoryName, msg)
		return &api.MkdirResponse{}, nil 
	}

	err := s.acquireLock(ctx, req.DirectoryName)
	if err != nil {
		return &api.MkdirResponse{Success: proto.Bool(false)}, nil
	}
	defer s.releaseLock(ctx, req.DirectoryName)

	s.logger.Infof("[DFS][Mkdir] Sending create (Put empty) request to extent service for directory: %s", req.DirectoryName)
	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{
		FileName: req.DirectoryName,
		FileData: []byte{},
	})
	if err != nil {
		s.logger.Errorf("[DFS][Mkdir] Extent service Put request failed for directory: %s, error: %v", req.DirectoryName, err)
		return &api.MkdirResponse{Success: proto.Bool(false)}, nil
	}

	if resp == nil || !(*resp.Success) {
		s.logger.Warnf("[DFS][Mkdir] Extent service Put returned failure for directory: %s", req.DirectoryName)
		return &api.MkdirResponse{Success: proto.Bool(false)}, nil
	}

	s.logger.Infof("[DFS][Mkdir] Directory %s created successfully", req.DirectoryName)
	return &api.MkdirResponse{Success: proto.Bool(true)}, nil
}

func (s *DfsServiceServer) Rmdir(ctx context.Context, req *api.RmdirRequest) (*api.RmdirResponse, error) {
	isCorrect, msg := s.validateName(req.DirectoryName, true)
	if !isCorrect {
		s.logger.Errorf("[DFS][Rmdir] Validation failed for directory %s: %s", req.DirectoryName, msg)
		return &api.RmdirResponse{}, nil 
	}

	err := s.acquireLock(ctx, req.DirectoryName)
	if err != nil {
		return &api.RmdirResponse{Success: proto.Bool(false)}, nil
	}
	defer s.releaseLock(ctx, req.DirectoryName)

	s.logger.Infof("[DFS][Rmdir] Sending delete (empty Put) request to extent service for directory: %s", req.DirectoryName)
	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{FileName: req.DirectoryName})
	if err != nil {
		s.logger.Errorf("[DFS][Rmdir] Extent service Put request failed for directory: %s, error: %v", req.DirectoryName, err)
		return &api.RmdirResponse{Success: proto.Bool(false)}, nil
	}

	if resp == nil || !(*resp.Success) {
		s.logger.Warnf("[DFS][Rmdir] Extent service Put returned failure for directory: %s", req.DirectoryName)
		return &api.RmdirResponse{Success: proto.Bool(false)}, nil
	}

	s.logger.Infof("[DFS][Rmdir] Directory %s removed successfully", req.DirectoryName)
	return &api.RmdirResponse{Success: proto.Bool(true)}, nil
}

func (s *DfsServiceServer) Put(ctx context.Context, req *api.PutRequest) (*api.PutResponse, error) {
	isCorrect, msg := s.validateName(req.FileName, false)
	if !isCorrect {
		s.logger.Errorf("[DFS][Put] Validation failed for file %s: %s", req.FileName, msg)
		return &api.PutResponse{}, nil 
	}

	err := s.acquireLock(ctx, req.FileName)
	if err != nil {
		s.logger.Errorf("[DFS][Put] failed to acquire lock for %s", req.FileName)
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}
	defer s.releaseLock(ctx, req.FileName)

	if req.FileData == nil {
		s.logger.Warnf("[DFS][Put] empty file data for %s", req.FileName)
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{
		FileName: req.FileName,
		FileData: req.FileData,
	})

	if err != nil || !(*resp.Success) {
		s.logger.Errorf("[DFS][Put] extent Put failed for %s: %v", req.FileName, err)
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	s.logger.Infof("[DFS][Put] file %s saved successfully", req.FileName)
	return &api.PutResponse{Success: proto.Bool(true)}, nil
}

func (s *DfsServiceServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	isCorrect, msg := s.validateName(req.FileName, false)
	if !isCorrect {
		s.logger.Errorf("[DFS][Get] Validation failed for file %s: %s", req.FileName, msg)
		return &api.GetResponse{}, nil 
	}

	err := s.acquireLock(ctx, req.FileName)
	if err != nil {
		return &api.GetResponse{}, nil
	}
	defer s.releaseLock(ctx, req.FileName)

	s.logger.Infof("[DFS][Get] Fetching file data from extent service: %s", req.FileName)
	resp, err := s.extentClient.Get(ctx, &extent.GetRequest{FileName: req.FileName})
	if err != nil {
		s.logger.Errorf("[DFS][Get] Extent service Get request failed for file: %s, error: %v", req.FileName, err)
		return &api.GetResponse{}, nil
	}
	if resp == nil || len(resp.FileData) == 0 {
		s.logger.Warnf("[DFS][Get] No data returned for file: %s", req.FileName)
		return &api.GetResponse{}, nil
	}

	s.logger.Infof("[DFS][Get] Successfully retrieved file: %s (size=%d bytes)", req.FileName, len(resp.FileData))
	return &api.GetResponse{FileData: resp.FileData}, nil

}

func (s *DfsServiceServer) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	s.logger.Infof("[DFS][Delete] Request received for file: %s", req.FileName)

	isCorrect, msg := s.validateName(req.FileName, false)
	if !isCorrect {
		s.logger.Errorf("[DFS][Delete] Validation failed for file %s: %s", req.FileName, msg)
		return &api.DeleteResponse{Success: proto.Bool(false)}, nil 
	}

	err := s.acquireLock(ctx, req.FileName)
	if err != nil {
		s.logger.Errorf("[DFS][Delete] Failed to acquire lock for file: %s, error: %v", req.FileName, err)
		return &api.DeleteResponse{Success: proto.Bool(false)}, nil
	}
	defer s.releaseLock(ctx, req.FileName)

	s.logger.Infof("[DFS][Delete] Sending delete (Put empty) request to extent service for file: %s", req.FileName)
	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{FileName: req.FileName})
	if err != nil {
		s.logger.Errorf("[DFS][Delete] Extent service Put request failed for file: %s, error: %v", req.FileName, err)
		return &api.DeleteResponse{Success: proto.Bool(false)}, nil
	}

	if resp == nil || !(*resp.Success) {
		s.logger.Warnf("[DFS][Delete] Extent service Put returned failure for file: %s", req.FileName)
		return &api.DeleteResponse{Success: proto.Bool(false)}, nil
	}

	s.logger.Infof("[DFS][Delete] File deleted successfully: %s", req.FileName)
	return &api.DeleteResponse{Success: proto.Bool(true)}, nil
}


// endregion

// region private method

func (s *DfsServiceServer) acquireLock(ctx context.Context, lockId string) error {
	seqNum := s.dfsClient.GetNewSeqNum()

	s.logger.Infof("[DFS] Trying to acquire lock: lock_id=%s owner_id=%s seq=%d", lockId, s.dfsClient.ownerId, seqNum)

	lockResp, err := s.cacheManager.Acquire(ctx, &lock.AcquireRequest{LockId: lockId, OwnerId: s.dfsClient.ownerId, Sequence: seqNum})

	if err != nil {
		s.logger.Errorf("[DFS] Failed to acquire lock %s: %v", lockId, err)
		return nil
	}

	if lockResp == nil || lockResp.Success == nil || !*lockResp.Success {
		s.logger.Warnf("[DFS] Lock %s not acquired (lock may be busy or denied)", lockId)
		return nil
	}

	s.logger.Infof("[DFS] Lock %s successfully acquired by %s (seq=%d)", lockId, s.dfsClient.ownerId, seqNum)
	return nil
}

func (s *DfsServiceServer) releaseLock(ctx context.Context, lockId string) {
	seq := s.dfsClient.GetCurrentSeqNum()

	s.logger.Infof("[DFS] Releasing lock: lock_id=%s owner_id=%s seq=%d",
		lockId, s.dfsClient.ownerId, seq)

	_, err := s.cacheManager.Release(ctx, &lock.ReleaseRequest{
		LockId:   lockId,
		OwnerId:  s.dfsClient.ownerId,
		Sequence: seq,
	})

	if err != nil {
		s.logger.Errorf("[DFS] Failed to release lock %s: %v", lockId, err)
		return
	}

	s.logger.Infof("Lock %s released successfully by %s", lockId, s.dfsClient.ownerId)
}

func (s *DfsServiceServer) validateName(name string, isDir bool) (bool, string) {
	if name == "" {
		return false, "invalid name: name is empty"
	}

	if isDir && !s.isDir(name) {
		return false, fmt.Sprintf("invalid name: expected directory but got file: %s", name)
	}

	if !isDir && s.isDir(name) {
		return false, fmt.Sprintf("invalid name: expected file but got directory: %s", name)
	}

	return true, ""
}

func (s *DfsServiceServer) isDir(path string) bool {
	return strings.HasSuffix(path, "/")
}

// endregion
