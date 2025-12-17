package replica

import (
	utils "dfs/utils"

	paxosApi "dfs/proto-gen/paxos"
)

func connectToAllPaxosClients(addrs []string) []paxosApi.PaxosServiceClient{
	clients := []paxosApi.PaxosServiceClient{}

	for _, addr := range addrs {
		client, err := utils.ConnectToPaxosClient(addr)
		if err != nil {
			continue
		}

		clients = append(clients, client)
	}

	return clients
}