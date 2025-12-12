package paxos

import (
	paxosApi "dfs/proto-gen/paxos"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func connectToAllPaxosClients(addrs []string) []paxosApi.PaxosServiceClient{
	clients := []paxosApi.PaxosServiceClient{}

	for _, addr := range addrs {
		client, err := connectToPaxosClient(addr)
		if err != nil {
			continue
		}

		clients = append(clients, client)
	}

	return clients
}

func connectToPaxosClient(addr string) (paxosApi.PaxosServiceClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return paxosApi.NewPaxosServiceClient(conn), nil
}