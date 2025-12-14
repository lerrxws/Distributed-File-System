package paxos

import (
	utils "dfs/utils"

	paxosApi "dfs/proto-gen/paxos"

	fl "dfs/utils/filelogger"
	
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

func (a *Acceptor) logToFile() error{
	timestamp := utils.GetTimeStamp()

	entry := fl.PaxosLogEntry{
		Timestamp: timestamp,
		Instance: a.viewId,
		NH: a.n_h,
		NA: a.n_a,
		VA: a.v_a,	
	}

	return a.filelogger.Append(entry)
}