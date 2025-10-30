package dfs

import (
	"context"
	"path/filepath"
	"strings"
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
	FileID string
	Data   []byte
	State  CacheState
	Parent *ExtentCache
}

func (c *ExtentCache) SetCacheData(data []byte) {
	c.Data = data
}

func (c *ExtentCache) SetCacheState(isDirty CacheState) {
	c.State = isDirty
}

func (c *ExtentCache) SetCacheParent(parent *ExtentCache) {
	c.Parent = parent
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
		FileID: fileID,
		Data:   data,
		State:  Clean,
	}
}

func (cm *ExtentCacheManager) AddLocalyCreatedFileToCache(fileID string, data []byte) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.cache[fileID] = &ExtentCache{
		FileID: fileID,
		Data:   data,
		State:  Dirty,
	}
}

func (cm *ExtentCacheManager) RemoveCache(fileID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.cache, fileID)
}

func (cm *ExtentCacheManager) UpdateCacheData(cache *ExtentCache, newCacheData []byte) {
	if cache == nil {
		return
	}
	cache.SetCacheData(newCacheData)
	cache.SetCacheState(Dirty)
}

func (cm *ExtentCacheManager) UpdateParentOnCreate(child *ExtentCache) {
	if child == nil || child.Parent == nil {
		return
	}

	parent := child.Parent
	parentData := parent.Data
	parentDataStr := string(parentData)
	childName := filepath.Base(child.FileID)

	if !strings.Contains(parentDataStr, childName) {
		if len(parentDataStr) > 0 && !strings.HasSuffix(parentDataStr, "\n") {
			parentDataStr += "\n"
		}
		parentDataStr += childName + "\n"
		parent.Data = []byte(childName)
		cm.logger.Infof("[ExtentCache] Added '%s' to parent '%s'.", childName, parent.FileID)
	}
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
	fileId := req.FileName

	cache, err := ec.getCacheFromRemoteServer(ctx, fileId)
	if err != nil {
		return nil, err
	}
	ec.logger.Infof("[ExtentCache] Returning cached data for '%s'.", req.FileName)

	parentId := getParentPath(fileId)
	parentCache, err := ec.getCacheFromRemoteServer(ctx, parentId)
	if err == nil {
		cache.Parent = parentCache
	}

	return &extentapi.GetResponse{
		FileData: cache.Data,
	}, nil
}

func getParentPath(path string) string {
	// Уніфікуємо роздільники
	path = strings.ReplaceAll(path, "\\", "/")

	// Якщо це вже root
	if path == "/" {
		return "/"
	}

	// Якщо це директорія (має закінчення "/") — прибираємо його тимчасово
	path = strings.TrimSuffix(path, "/")

	// Знаходимо останній "/"
	idx := strings.LastIndex(path, "/")
	if idx == -1 {
		return "/"
	}

	// Беремо все до останнього "/" включно — це і є parent dir
	parent := path[:idx+1]
	return parent
}


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

// Put() - create, update or delete file only in cache
func (ec *ExtentCacheHandler) Put(ctx context.Context, req *extentapi.PutRequest, opts ...grpc.CallOption) (*extentapi.PutResponse, error) {
	ec.logger.Infof("[ExtentCache] Received Put request for '%s'.", req.FileName)

	_, err := ec.Get(ctx, &extentapi.GetRequest{FileName: req.FileName})
	if err != nil {
		ec.logger.Errorf("[ExtentCache] Failed to get existing file '%s' from cache or server: %v", req.FileName, err)
		return nil, err
	}

	if req.FileData == nil {
		ec.logger.Infof("[ExtentCache] No file-data provided for '%s' — performing delete.", req.FileName)
		return ec.deleteFile(req.FileName)
	}

	ec.logger.Infof("[ExtentCache] Updating file-data for '%s'.", req.FileName)
	return ec.updateFile(req.FileName, req.FileData)
}

func (ec *ExtentCacheHandler) Stop(ctx context.Context, req *extentapi.StopRequest, opts ...grpc.CallOption) (*extentapi.StopResponse, error) {
	return ec.extentClient.Stop(ctx, req, opts...)
}

func (ec *ExtentCacheHandler) Update(ctx context.Context, fileId string) {
	cache, exist := ec.cacheManager.GetCache(fileId)
	if !exist {
		ec.logger.Warnf("[ExtentCache] File '%s' not in cache — skipping update.", fileId)
		return
	}
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
	ec.logger.Infof("[ExtentCache] Successfully synchronized dirty file '%s' with server.", cache.FileID)

	parentCache := cache.Parent
	if parentCache == nil || parentCache.State == Clean {
		return
	}

	putResp, err = ec.extentClient.Put(ctx, &extentapi.PutRequest{
		FileName: parentCache.FileID,
		FileData: parentCache.Data,
	})
	if err != nil {
		ec.logger.Errorf("[ExtentCache] Failed to update parent '%s': %v", parentCache.FileID, err)
		return
	}
	if putResp == nil || !(*putResp.Success) {
		ec.logger.Warnf("[ExtentCache] Unsuccessful response updating parent '%s'.", parentCache.FileID)
		return
	}

	parentCache.SetCacheState(Clean)
	ec.logger.Infof("[ExtentCache] Successfully synchronized parent '%s' with server.", parentCache.FileID)
}

func (ec *ExtentCacheHandler) Flush(fileId string) {
	cache, exist := ec.cacheManager.GetCache(fileId)
	if !exist {
		ec.logger.Warnf("[ExtentCache] File '%s' not in cache — skipping flush.", fileId)
		return
	}

	if cache.State == Dirty {
		ec.logger.Warnf("[ExtentCache] File '%s' is still dirty — flush should occur only after update.", fileId)
		return
	}

	ec.cacheManager.RemoveCache(fileId)
	ec.logger.Infof("[ExtentCache] Successfully flushed '%s' from local cache.", fileId)

	parentCache := cache.Parent
	if parentCache == nil || parentCache.State == Dirty {
		return
	}

	ec.cacheManager.RemoveCache(parentCache.FileID)
	ec.logger.Infof("[ExtentCache] Successfully flushed parent '%s' from local cache.", parentCache.FileID)
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
