package replica

import (
	utils "dfs/utils"
	fl "dfs/utils/filelogger"
	
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

func (r *ReplicaServiceServer) logToFileExecutedMethod(methodReq *replicaApi.MethodRequest) error{
	timestamp := utils.GetTimeStamp()

	entry := fl.MethodLogEntry{
		Timestamp: timestamp,
		Method: methodReq.MethodName,
		Params: methodReq.MethodParameters,
	}

	return r.filelogger.Append(entry)
}