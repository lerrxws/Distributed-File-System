package dfs

type CacheState string

const (
	None      CacheState = "None"
	Free      CacheState = "Free"
	Locked    CacheState = "Locked"
	Releasing CacheState = "Releasing"
	Acquiring CacheState = "Acquiring"
)
