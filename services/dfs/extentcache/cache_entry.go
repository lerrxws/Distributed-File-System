package dfs

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