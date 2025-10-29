package dfs

import (
	"context"
	extentapi "dfs/proto-gen/extent"
)

type ExtentCache struct {
	extentClient extentapi.ExtentServiceClient
}

func NewExtentCache(extentClient extentapi.ExtentServiceClient) *ExtentCache{
	return &ExtentCache{
		extentClient: extentClient,
	}
}

func (ec *ExtentCache) Get(ctx context.Context, req *extentapi.GetRequest) (* extentapi.GetResponse, error){
	return ec.extentClient.Get(ctx, req)
}

func (ec *ExtentCache) Put(ctx context.Context, req *extentapi.PutRequest) (* extentapi.PutResponse, error){
	return ec.extentClient.Put(ctx, req)
}