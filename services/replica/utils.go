package replica

import (
	utils "dfs/utils"
	
	replicaApi "dfs/proto-gen/replica"
)

func (r *ReplicaServiceServer) connectToPrimary(primaryAddr string) (replicaApi.ReplicaServiceClient, error) {
	client, err := utils.ConnectToReplicaClient(primaryAddr)
	if err != nil {
		return nil, r.logger.Errorf("failed to connect to primary node %s: %v", primaryAddr, err)
	}
	return client, nil
}

func IsPrimaryAddressSet(primaryAddr string) bool {
	return primaryAddr != ""
}