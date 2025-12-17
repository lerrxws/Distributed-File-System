package proxy

import (
	"strconv"

	lock "dfs/proto-gen/lock"
	replica "dfs/proto-gen/replica"
)

func buildAcquireRequest(req *lock.AcquireRequest, viewId int64, sequence int64) *replica.ExecuteMethodRequest {
	paramStr := []string{
		req.LockId,
		req.OwnerId,
		strconv.FormatInt(req.Sequence, 10),
	}

	return buildRequest(paramStr, viewId, "Acquire", sequence)
}

func buildReleaseRequest(req *lock.ReleaseRequest, viewId int64, sequence int64) *replica.ExecuteMethodRequest {
	paramStr := []string{
		req.LockId,
		req.OwnerId,
		strconv.FormatInt(req.Sequence, 10),
	}

	return buildRequest(paramStr, viewId, "Release", sequence)
}

func buildStopRequest(req *lock.StopRequest, viewId int64, sequence int64) *replica.ExecuteMethodRequest{
	paramStr := []string{}

	return buildRequest(paramStr, viewId, "Stop", sequence)
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
