package dfs

type CacheState bool

const (
	Clean CacheState = true
	Dirty CacheState = false
)