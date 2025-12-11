package paxos

import (
	paxosApi "dfs/proto-gen/paxos"

	seelog "github.com/cihub/seelog"
)

type PaxosHelper struct {
	proposer *Proposer
	acceptor *Acceptor

	logger seelog.LoggerInterface
}

func NewPaxosService(addr string, initialViewId int64, initialView []string, logger seelog.LoggerInterface) *PaxosHelper {
	return &PaxosHelper{
		proposer: NewProposer(addr, logger),
		acceptor: NewAcceptor(initialViewId, initialView, logger),

		logger: logger,
	}
}

func (p *PaxosHelper) Propose(vId int64, vOld []string, v []string) (int64, []string) {
	return p.proposer.Propose(vId, vOld, v)
}

func (p *PaxosHelper) Prepare(req *paxosApi.PrepareRequest, node string) (*paxosApi.PrepareResponse, error) {
	return p.acceptor.Prepare(req)
}

func (p *PaxosHelper) Accept(req *paxosApi.AcceptRequest, node string) (*paxosApi.AcceptResponse, error) {
	return p.acceptor.Accept(req)
}

func (p *PaxosHelper) Decide(req *paxosApi.DecideRequest, node string) (*paxosApi.DecideResponse, error) {
	return p.acceptor.Decide(req)
}

