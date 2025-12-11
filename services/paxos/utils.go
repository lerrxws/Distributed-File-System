package paxos

import (
	"hash/fnv"

	paxosApi "dfs/proto-gen/paxos"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func nodeIdToUint16(id string) uint16 {
	h := fnv.New32a()
	h.Write([]byte(id))
	return uint16(h.Sum32())
}

func connectToNode(addr string) paxosApi.PaxosServiceClient {
	conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return paxosApi.NewPaxosServiceClient(conn)
}