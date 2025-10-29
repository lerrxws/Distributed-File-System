package dfs

import (
	"context"
	"fmt"
	"strings"
	"sync"

	api "dfs/proto-gen/dfs"
	extent "dfs/proto-gen/extent"
	lock "dfs/proto-gen/lock"

	lockcache "dfs/services/lockcache"

	seelog "github.com/cihub/seelog"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type DfsClient struct {
	ownerId string
	seqNum  int64

	mu sync.Mutex
}

func NewDfsClient(port string) *DfsClient {
	return &DfsClient{
		ownerId: fmt.Sprintf("127.0.0.1:%s:DfsClient", port),
		seqNum:  0,
	}
}

func (dc *DfsClient) GetNewSeqNum() int64 {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	dc.seqNum++
	return dc.seqNum
}

func (dc *DfsClient) GetCurrentSeqNum() int64 {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	return dc.seqNum
}

type DfsServiceServer struct {
	lockClient   lock.LockServiceClient
	cacheMangaer *lockcache.CacheManager
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
		cacheMangaer: cacheManager,

		extentClient: extentClient,
		dfsClient:    dfsClient,
		grpc:         grpcServer,
		logger:       logger,
	}, nil
}

func (s *DfsServiceServer) acquireLock(ctx context.Context, lockId string) error {
	seqNum := s.dfsClient.GetNewSeqNum()

	s.logger.Infof("[DFS] Trying to acquire lock: lock_id=%s owner_id=%s seq=%d", lockId, s.dfsClient.ownerId, seqNum)

	lockResp, err := s.cacheMangaer.Acquire(ctx, &lock.AcquireRequest{LockId: lockId, OwnerId: s.dfsClient.ownerId, Sequence: seqNum})

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

	_, err := s.cacheMangaer.Release(ctx, &lock.ReleaseRequest{
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

	err := s.acquireLock(ctx, req.DirectoryName)
	if err != nil {
		return &api.DirResponse{Success: proto.Bool(false)}, nil
	}

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

	err := s.acquireLock(ctx, req.DirectoryName)
	if err != nil {
		return &api.MkdirResponse{Success: proto.Bool(false)}, nil
	}
	defer s.releaseLock(ctx, req.DirectoryName)

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

	err := s.acquireLock(ctx, req.DirectoryName)
	if err != nil {
		return &api.RmdirResponse{Success: proto.Bool(false)}, nil
	}
	defer s.releaseLock(ctx, req.DirectoryName)

	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{FileName: req.DirectoryName})
	if err != nil || !(*resp.Success) {
		return &api.RmdirResponse{Success: proto.Bool(false)}, nil
	}

	return &api.RmdirResponse{Success: proto.Bool(true)}, nil
}

func (s *DfsServiceServer) Put(ctx context.Context, req *api.PutRequest) (*api.PutResponse, error) {
	s.logger.Infof("[DFS] Put request: %s", req.FileName)
	if req.FileName == "" || strings.HasSuffix(req.FileName, "/") {
		s.logger.Warnf("[DFS] Put: invalid file name %s", req.FileName)
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	err := s.acquireLock(ctx, req.FileName)
	if err != nil {
		s.logger.Errorf("[DFS] Put: failed to acquire lock for %s", req.FileName)
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}
	defer s.releaseLock(ctx, req.FileName)

	if req.FileData == nil {
		s.logger.Warnf("[DFS] Put: empty file data for %s", req.FileName)
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{
		FileName: req.FileName,
		FileData: req.FileData,
	})
	if err != nil || !(*resp.Success) {
		s.logger.Errorf("[DFS] Put: extent Put failed for %s: %v", req.FileName, err)
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	s.logger.Infof("[DFS] Put: file %s saved successfully", req.FileName)
	return &api.PutResponse{Success: proto.Bool(true)}, nil
}

func (s *DfsServiceServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	if req.FileName == "" {
		return &api.GetResponse{}, nil
	}

	if strings.HasSuffix(req.FileName, "/") {
		return &api.GetResponse{}, nil
	}

	err := s.acquireLock(ctx, req.FileName)
	if err != nil {
		return &api.GetResponse{}, nil
	}
	defer s.releaseLock(ctx, req.FileName)

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

	err := s.acquireLock(ctx, req.FileName)
	if err != nil {
		return &api.DeleteResponse{Success: proto.Bool(false)}, nil
	}
	defer s.releaseLock(ctx, req.FileName)

	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{FileName: req.FileName})
	if err != nil || !(*resp.Success) {
		return &api.DeleteResponse{Success: proto.Bool(false)}, nil
	}

	return &api.DeleteResponse{Success: proto.Bool(true)}, nil
}
