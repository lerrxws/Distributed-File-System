package dfs

import (
	"path/filepath"
	"strings"
	"sync"

	seelog "github.com/cihub/seelog"
)

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

// region public methods

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

//endregion