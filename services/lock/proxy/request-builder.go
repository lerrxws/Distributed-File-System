package proxy

import (
	"strconv"

	lock "dfs/proto-gen/lock"
	replica "dfs/proto-gen/replica"
)

func buildAcquireRequest(req *lock.AcquireRequest, viewId int64) *replica.ExecuteMethodRequest {
	paramStr := []string{
		req.LockId,
		req.OwnerId,
		strconv.FormatInt(req.Sequence, 10),
	}

	return buildRequest(paramStr, viewId, "Acquire", req.Sequence)
}

func buildReleaseRequest(req *lock.ReleaseRequest, viewId int64) *replica.ExecuteMethodRequest {
	paramStr := []string{
		req.LockId,
		req.OwnerId,
		strconv.FormatInt(req.Sequence, 10),
	}

	return buildRequest(paramStr, viewId, "Release", req.Sequence)
}

func buildStopRequest(req *lock.StopRequest, viewId int64) *replica.ExecuteMethodRequest{
	paramStr := []string{}

	return buildRequest(paramStr, viewId, "Stop", -1)
}

func buildRequest(paramsStr []string, viewId int64, requestName string, sequence int64) *replica.ExecuteMethodRequest {
	methodReq := replica.MethodRequest{
		MethodName:       requestName,
		MethodParameters: paramsStr,
	}

	viewstamp := replica.Viewstamp{
		ViewId:   viewId,
		Sequence: sequence,
	}

	return &replica.ExecuteMethodRequest{
		MethodRequest: &methodReq,
		Viewstamp:     &viewstamp,
	}
}

