package replica

import (
	"context"

	paxosApi "dfs/proto-gen/paxos"
)

func (r *ReplicaServiceServer) Prepare(ctx context.Context, req *paxosApi.PrepareRequest) (*paxosApi.PrepareResponse, error) {
	r.logger.Infof("[Replica] Received Prepare request.")
	return r.manager.Acceptor.Prepare(req)
}

func (r *ReplicaServiceServer) Accept(ctx context.Context, req *paxosApi.AcceptRequest) (*paxosApi.AcceptResponse, error) {
	r.logger.Infof("[Replica] Received Access request.")
	return r.manager.Acceptor.Accept(req)
}

func (r *ReplicaServiceServer) Decide(ctx context.Context, req *paxosApi.DecideRequest) (*paxosApi.DecideResponse, error) {
	r.logger.Infof("[Replica] Received Decide request.")

	isAccepted, err := r.manager.Acceptor.Decide(req)
	if err != nil {
		r.logger.Errorf("[Replica] Error during Decided process: %v", err)
		return nil, r.logger.Errorf("error during Decide: %v", err)
	}

	if isAccepted {
		go r.recover()
	}

	return &paxosApi.DecideResponse{}, nil
}