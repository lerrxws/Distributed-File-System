package dfs

import (
	"context"
	"sync"

	extentapi "dfs/proto-gen/extent"

	seelog "github.com/cihub/seelog"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type CacheState bool

const (
	Clean CacheState = true
	Dirty CacheState = false
)

// Cached extent entry
type ExtentCache struct {
	FileID   string
	Data     []byte
	IsDirty  CacheState
	ParentID string
}

func (c *ExtentCache) SetCacheData(data []byte) {
	c.Data = data
}

func (c *ExtentCache) SetCacheDirtyState(isDirty CacheState) {
	c.IsDirty = isDirty
}

type ExtentCacheManager struct {
	mu     sync.Mutex
	logger seelog.LoggerInterface
	cache  map[string]*ExtentCache
}

// Constructor
func NewCacheManager(logger seelog.LoggerInterface) *ExtentCacheManager {
	return &ExtentCacheManager{
		logger: logger,
		cache:  make(map[string]*ExtentCache),
	}
}

func (cm *ExtentCacheManager) IsCached(fileID string) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	_, exist := cm.cache[fileID]
	return exist
}

func (cm *ExtentCacheManager) GetCache(fileID string) (*ExtentCache, bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cache, exist := cm.cache[fileID]
	return cache, exist
}

func (cm *ExtentCacheManager) AddExistingFileToCache(fileID string, data []byte) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.cache[fileID] = &ExtentCache{
		FileID:  fileID,
		Data:    data,
		IsDirty: Clean,
	}
}

func (cm *ExtentCacheManager) AddLocalyCreatedFileToCache(fileID string, data []byte) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.cache[fileID] = &ExtentCache{
		FileID:  fileID,
		Data:    data,
		IsDirty: Dirty,
	}
}

func (cm *ExtentCacheManager) RemoveCache(fileID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.cache, fileID)
}

func (cm *ExtentCacheManager) UpdateCacheData(fileId string, newCacheData []byte) {
	cache, exist := cm.GetCache(fileId)
	if !exist {
		return
	}

	cache.SetCacheData(newCacheData)
	cache.SetCacheDirtyState(Dirty)
}

type ExtentCacheHandler struct {
	cacheManager *ExtentCacheManager
	extentClient extentapi.ExtentServiceClient
	logger       seelog.LoggerInterface
}

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
	// Check cache
	if !ec.cacheManager.IsCached(req.FileName) {
		getResp, err := ec.extentClient.Get(ctx, req)
		if err != nil {
			ec.logger.Errorf("[ExtentCache] failed to get file-data from server: %v", err)
			return nil, err
		}

		if getResp.FileData == nil {
			ec.logger.Warnf("[ExtentCache] empty file-data for %s", req.FileName)
			getResp.FileData = []byte{}
		}

		// Add to cache
		ec.logger.Warnf("[ExtentCache] adding to cache file-data for %s", req.FileName)
		ec.cacheManager.AddExistingFileToCache(req.FileName, getResp.FileData)

		ec.logger.Infof("[ExtentCache] cached new extent: %s", req.FileName)
	}

	cache, _ := ec.cacheManager.GetCache(req.FileName)
	ec.logger.Infof("[ExtentCache] returning cached data for %s", req.FileName)

	return &extentapi.GetResponse{
		FileData: cache.Data,
	}, nil
}

// Put() - create, update or delete file only in cache
func (ec *ExtentCacheHandler) Put(ctx context.Context, req *extentapi.PutRequest, opts ...grpc.CallOption) (*extentapi.PutResponse, error) {
	ec.logger.Infof("[ExtentCache] Put request received for file %s", req.FileName)

	_, err := ec.Get(ctx, &extentapi.GetRequest{FileName: req.FileName})
	if err != nil {
		ec.logger.Errorf("[ExtentCache] failed to get existing file %s from cache or server: %v", req.FileName, err)
		return nil, err
	}

	if req.FileData == nil {
		ec.logger.Infof("[ExtentCache] no file-data is present for file %s - performing deleting", req.FileName)
		return ec.deleteFile(req.FileName)
	}

	ec.logger.Infof("[ExtentCache] put new file-data for file %s", req.FileName)
	return ec.updateFile(req.FileName, req.FileData)
}

func (ec *ExtentCacheHandler) Stop(ctx context.Context, req *extentapi.StopRequest, opts ...grpc.CallOption) (*extentapi.StopResponse, error) {
	return ec.extentClient.Stop(ctx, req, opts...)
}

func (ec *ExtentCacheHandler) Update(ctx context.Context, fileId string) {
	cache, exist := ec.cacheManager.GetCache(fileId)
	if !exist {
		ec.logger.Warnf("[ExtentCache] file '%s' not in cache — skipping update", fileId)
		return
	}
	if cache.IsDirty == Clean {
		return
	}

	putResp, err := ec.extentClient.Put(ctx, &extentapi.PutRequest{
		FileName: cache.FileID,
		FileData: cache.Data,
	})
	if err != nil {
		ec.logger.Errorf("[ExtentCache] failed to update dirty file '%s': %v", cache.FileID, err)
		return
	}
	if putResp == nil || !(*putResp.Success) {
		ec.logger.Warnf("[ExtentCache] unsuccessful response updating file '%s'", cache.FileID)
		return
	}

	cache.IsDirty = Clean
	ec.logger.Infof("[ExtentCache] successfully synchronized dirty file '%s' with main server", cache.FileID)
}

func (ec *ExtentCacheHandler) Flush(fileId string) {
	cache, exist := ec.cacheManager.GetCache(fileId)
	if !exist {
		ec.logger.Warnf("[ExtentCache] file '%s' is not present in cache — skipping flush", fileId)
		return
	}

	if cache.IsDirty == Dirty {
		ec.logger.Warnf("[ExtentCache] file '%s' is still dirty — flush should only occur after successful update", fileId)
		return
	}

	ec.cacheManager.RemoveCache(fileId)
	ec.logger.Infof("[ExtentCache] successfully flushed file '%s' from local cache", fileId)
}

func (ec *ExtentCacheHandler) updateFile(FileName string, FileData []byte) (*extentapi.PutResponse, error) {
	_, exist := ec.cacheManager.GetCache(FileName)
	if !exist {
		ec.cacheManager.AddLocalyCreatedFileToCache(FileName, FileData)

		ec.logger.Info("[ExtentCache] adding new file %s to cache", FileName)
		return &extentapi.PutResponse{Success: proto.Bool(true)}, nil
	}
	ec.logger.Info("[ExtentCache] file %s is present in cache - updating its data", FileName)
	ec.cacheManager.UpdateCacheData(FileName, FileData)

	return &extentapi.PutResponse{Success: proto.Bool(true)}, nil
}

func (ec *ExtentCacheHandler) deleteFile(FileName string) (*extentapi.PutResponse, error) {
	_, exist := ec.cacheManager.GetCache(FileName)
	if !exist {
		ec.logger.Errorf("[ExtentCache] file %s does not exist - unable to perform Delete operation", FileName)
		return &extentapi.PutResponse{Success: proto.Bool(false)}, nil
	}
	ec.logger.Info("[ExtentCache] deleting %s file is successful", FileName)
	ec.cacheManager.UpdateCacheData(FileName, nil)

	return &extentapi.PutResponse{Success: proto.Bool(true)}, nil
}
