package replica

import (
	"context"

	managementApi "dfs/proto-gen/management"
)

func (r *ReplicaServiceServer) Heartbeat(ctx context.Context, req *managementApi.HeartbeatRequest) (*managementApi.HeartbeatResponse, error) {
	r.logger.Infof("[Management] Get HeartBeat from node %s.", req.Sender)
	return &managementApi.HeartbeatResponse{}, nil
}