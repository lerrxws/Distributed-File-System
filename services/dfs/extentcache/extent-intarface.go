package dfs

import (
	"context"
	extentapi "dfs/proto-gen/extent"
)

type ExtentInterface interface {
	Get(ctx context.Context, req *extentapi.GetRequest) (* extentapi.GetResponse, error)
	Put(ctx context.Context, req *extentapi.PutRequest) (* extentapi.PutResponse, error)
}