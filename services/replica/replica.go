package replica

import (
	"context"

	paxosApi "dfs/proto-gen/paxos"
	replicaApi "dfs/proto-gen/replica"

	paxos "dfs/services/replica/paxos"

	"github.com/cihub/seelog"
	"google.golang.org/grpc"
)

type ReplicaServiceServer struct {
	grpc *grpc.Server

	addr      string
	isPrimary bool

	viewId int64
	view   []string

	proposer paxos.Proposer
	acceptor paxos.Acceptor

	logger seelog.LoggerInterface

	replicaApi.UnimplementedReplicaServiceServer
	paxosApi.UnimplementedPaxosServiceServer
}

func NewReplicaServiceServer(
	grpc *grpc.Server,
	addr string,
	logger seelog.LoggerInterface,

	primaryAddr string,
) *ReplicaServiceServer {
	r := &ReplicaServiceServer{
		grpc: grpc,

		addr:   addr,
		logger: logger,

		view:   []string{addr},
		viewId: 1,
	}

	r.acceptor = *paxos.NewAcceptor(r.viewId, r.view, r.commitView, r.logger)
	r.proposer = *paxos.NewProposer(r.commitView, logger)

	if !isPrimaryAddressSet(primaryAddr){
		r.isPrimary = true
	} else {
		r.joinView(primaryAddr)
	}

	return r
}

func (s *ReplicaServiceServer) GetView(ctx context.Context, req *replicaApi.GetViewRequest) (*replicaApi.GetViewResponse, error) {
	s.logger.Infof("[Replica] Received GetView request.")

	return &replicaApi.GetViewResponse{
		ViewId: s.viewId,
		View:   s.view,
	}, nil
}

func (r *ReplicaServiceServer) Prepare(ctx context.Context, req *paxosApi.PrepareRequest) (*paxosApi.PrepareResponse, error) {
	r.logger.Infof("[Replica] Received Prepare request.")
	return r.acceptor.Prepare(req)
}

func (r *ReplicaServiceServer) Accept(ctx context.Context, req *paxosApi.AcceptRequest) (*paxosApi.AcceptResponse, error) {
	r.logger.Infof("[Replica] Received Access request.")
	return r.acceptor.Accept(req)
}

func (r *ReplicaServiceServer) Decide(ctx context.Context, req *paxosApi.DecideRequest) (*paxosApi.DecideResponse, error) {
	r.logger.Infof("[Replica] Received Decide request.")
	resp, err := r.acceptor.Decide(req)

	return resp, err
}

func (r *ReplicaServiceServer) commitView(viewId int64, view []string) {
	r.viewId = viewId
	r.view = view
}

func (r *ReplicaServiceServer) joinView(primaryAddr string) {
	replicaClient, err := connectToReplicaClient(primaryAddr)
	if err != nil {
		r.logger.Infof("[Replica] Error while connecting to primary node %s. Becoming the primary node.", primaryAddr)
		r.isPrimary = true
		return
	}

	resp, err := replicaClient.GetView(context.Background(), &replicaApi.GetViewRequest{})
	if err != nil {
		r.logger.Infof("[Replica] Error while synchronizing view with primary node %s. Becoming the primary node.", primaryAddr)
		r.isPrimary = true
		return
	}
	
	newViewId := resp.ViewId + 1
	newView := append(resp.View, r.addr)

	go r.proposer.Propose(newViewId, resp.View, newView)
}
