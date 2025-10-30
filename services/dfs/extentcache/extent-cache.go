package dfs

import (
	"context"

	extentapi "dfs/proto-gen/extent"

	seelog "github.com/cihub/seelog"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type ExtentCacheHandler struct {
	cacheManager *ExtentCacheManager
	extentClient extentapi.ExtentServiceClient
	logger       seelog.LoggerInterface
}

// region public methods

// Constructor
func NewExtentCacheHandler(extentClient extentapi.ExtentServiceClient, cacheManager *ExtentCacheManager, logger seelog.LoggerInterface) *ExtentCacheHandler {
	return &ExtentCacheHandler{
		extentClient: extentClient,
		cacheManager: cacheManager,
		logger:       logger,
	}
}

// Get() — returns file data from cache if exists, otherwise from server
func (ec *ExtentCacheHandler) Get(ctx context.Context, req *extentapi.GetRequest, opts ...grpc.CallOption) (*extentapi.GetResponse, error) {
	fileId := req.FileName

	cache, err := ec.getCacheFromRemoteServer(ctx, fileId)
	if err != nil {
		return nil, err
	}

	parentId := getParentPath(fileId)
	parentCache, err := ec.getCacheFromRemoteServer(ctx, parentId)
	if err == nil {
		cache.Parent = parentCache
	}

	return &extentapi.GetResponse{
		FileData: cache.Data,
	}, nil
}

// Put() - create, update or delete file only in cache
func (ec *ExtentCacheHandler) Put(ctx context.Context, req *extentapi.PutRequest, opts ...grpc.CallOption) (*extentapi.PutResponse, error) {
	ec.logger.Infof("[ExtentCache][Put] Received Put request for '%s'.", req.FileName)

	_, err := ec.Get(ctx, &extentapi.GetRequest{FileName: req.FileName})
	if err != nil {
		ec.logger.Errorf("[ExtentCache][Put] Failed to get existing file '%s' from cache or server: %v", req.FileName, err)
		return nil, err
	}

	if req.FileData == nil {
		ec.logger.Infof("[ExtentCache][Put] No file-data provided for '%s' — performing delete.", req.FileName)
		return ec.deleteFile(req.FileName)
	}

	ec.logger.Infof("[ExtentCache][Put] Updating file-data for '%s'.", req.FileName)
	return ec.updateFile(req.FileName, req.FileData)
}

func (ec *ExtentCacheHandler) Stop(ctx context.Context, req *extentapi.StopRequest, opts ...grpc.CallOption) (*extentapi.StopResponse, error) {
	return ec.extentClient.Stop(ctx, req, opts...)
}

func (ec *ExtentCacheHandler) Update(ctx context.Context, fileId string) {
	cache, exist := ec.cacheManager.GetCache(fileId)
	if !exist {
		ec.logger.Warnf("[ExtentCache][Update] File '%s' not in cache — skipping update.", fileId)
		return
	}
	
	ec.handleUpdate(ctx, cache)
	ec.logger.Infof("[ExtentCache][Update] Successfully synchronized dirty file '%s' with server.", cache.FileID)

	parentCache := cache.Parent
	if parentCache == nil{
		return
	}
	ec.handleUpdate(ctx, parentCache)
	ec.logger.Infof("[ExtentCache][Update] Successfully synchronized parent '%s' with server.", parentCache.FileID)
}

func (ec *ExtentCacheHandler) Flush(fileId string) {
	cache, exist := ec.cacheManager.GetCache(fileId)
	if !exist {
		ec.logger.Warnf("[ExtentCache][Flush] File '%s' not in cache — skipping flush.", fileId)
		return
	}

	ec.handleFlash(cache)
	ec.logger.Infof("[ExtentCache][Flush] Successfully flushed '%s' from local cache.", fileId)

	parentCache := cache.Parent
	if parentCache == nil{
		return
	}

	ec.handleFlash(parentCache)
	ec.logger.Infof("[ExtentCache][Flush] Successfully flushed parent '%s' from local cache.", parentCache.FileID)
}

// endregion

// region private methods
func (ec *ExtentCacheHandler) getCacheFromRemoteServer(ctx context.Context, fileId string) (*ExtentCache, error) {
	if !ec.cacheManager.IsCached(fileId) {
		getResp, err := ec.extentClient.Get(ctx, &extentapi.GetRequest{
			FileName: fileId,
		})
		if err != nil {
			ec.logger.Errorf("[ExtentCache] Failed to get file-data from server for '%s': %v", fileId, err)
			return nil, err
		}

		if getResp.FileData == nil {
			ec.logger.Warnf("[ExtentCache] Empty file-data for '%s'.", fileId)
			getResp.FileData = []byte{}
		}

		ec.logger.Infof("[ExtentCache] Caching new file-data for '%s'.", fileId)
		ec.cacheManager.AddExistingFileToCache(fileId, getResp.FileData)
	}

	cache, _ := ec.cacheManager.GetCache(fileId)
	return cache, nil
}

func (ec *ExtentCacheHandler) updateFile(FileName string, FileData []byte) (*extentapi.PutResponse, error) {
	cache, exist := ec.cacheManager.GetCache(FileName)
	if !exist {
		ec.cacheManager.AddLocalyCreatedFileToCache(FileName, FileData)
		ec.logger.Infof("[ExtentCache] Added new file '%s' to cache.", FileName)
		return &extentapi.PutResponse{Success: proto.Bool(true)}, nil
	}
	ec.logger.Infof("[ExtentCache] Updating cached file '%s'.", FileName)
	ec.cacheManager.UpdateCacheData(cache, FileData)
	ec.cacheManager.UpdateParentOnCreate(cache)

	return &extentapi.PutResponse{Success: proto.Bool(true)}, nil
}

func (ec *ExtentCacheHandler) deleteFile(FileName string) (*extentapi.PutResponse, error) {
	cache, exist := ec.cacheManager.GetCache(FileName)
	if !exist {
		ec.logger.Errorf("[ExtentCache] File '%s' does not exist — unable to delete.", FileName)
		return &extentapi.PutResponse{Success: proto.Bool(false)}, nil
	}
	ec.logger.Infof("[ExtentCache] Deleted file '%s' successfully.", FileName)
	ec.cacheManager.UpdateCacheData(cache, nil)

	return &extentapi.PutResponse{Success: proto.Bool(true)}, nil
}

func (ec *ExtentCacheHandler) handleUpdate(ctx context.Context, cache *ExtentCache) {
	if cache.State == Clean {
		return
	}

	putResp, err := ec.extentClient.Put(ctx, &extentapi.PutRequest{
		FileName: cache.FileID,
		FileData: cache.Data,
	})
	if err != nil {
		ec.logger.Errorf("[ExtentCache] Failed to synchronize dirty file '%s': %v", cache.FileID, err)
		return
	}
	if putResp == nil || !(*putResp.Success) {
		ec.logger.Warnf("[ExtentCache] Unsuccessful response while updating '%s'.", cache.FileID)
		return
	}

	cache.SetCacheState(Clean)
}

func (ec *ExtentCacheHandler) handleFlash(cache *ExtentCache) {
	if cache.State == Dirty {
		ec.logger.Warnf("[ExtentCache] File '%s' is still dirty — flush should occur only after update.", cache.FileID)
		return
	}

	ec.cacheManager.RemoveCache(cache.FileID)
}

// endregion