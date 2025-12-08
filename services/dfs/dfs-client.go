package dfs

import (
	"fmt"
	"sync"
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

// region public methods

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

//endregion