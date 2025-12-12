package replica

import (
	replicaApi "dfs/proto-gen/replica"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func isPrimaryAddressSet(primaryAddr string) bool {
    return primaryAddr != ""
}

func connectToReplicaClient(addr string) (replicaApi.ReplicaServiceClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return replicaApi.NewReplicaServiceClient(conn), nil
}