package paxos

import (
	"context"

	paxosApi "dfs/proto-gen/paxos"

	seelog "github.com/cihub/seelog"
)

type Proposer struct {
	proposalCounter int64
	nodeIdHash      int64

	logger seelog.LoggerInterface
}

func NewProposer(addr string, logger seelog.LoggerInterface) *Proposer{
	return &Proposer{
		proposalCounter: 1,
		nodeIdHash: int64(nodeIdToUint16(addr)),

		logger: logger,
	}
}

func (p *Proposer) Propose(vId int64, vOld []string, v []string) (int64, []string) {
	n := p.nextProposalNumber()
	majority := len(vOld)/2 + 1

	prepareOk := 0
	for _, node := range vOld {
		p.logger.Infof("[Paxos] Prepare: Connecting to node: %s...", node)
		client := connectToNode(node)

		p.logger.Infof("[Paxos] Calling Prepare() on node: %s...", node)
		resp, err := client.Prepare(context.Background(), &paxosApi.PrepareRequest{
			ViewId: vId,
			N:      n,
		})

		if err != nil {
			p.logger.Errorf("[Paxos] Error during PREPARE for node %s: %v.", node, err)
			continue
		}

		if resp.IsOld {
			p.logger.Infof("[Paxos] Received OLDINSTANCE for node %s.", node)
			return resp.ViewId, resp.View
		}

		if resp.Success {
			p.logger.Infof("[Paxos] Received PREPARE-SUCCESS for node %s.", node)
			prepareOk++
		}
	}

	p.logger.Infof("[Paxos] Accepted prepare-ok (%d) with majority (%d).", prepareOk, majority)

	if prepareOk < majority {
		p.logger.Infof("[Paxos] Prepare (%d) rejected, not enough votes for majority (received %d, required %d).", vId, prepareOk, majority)
		return vId, vOld
	}

	acceptOk := 0
	for _, node := range vOld {
		p.logger.Infof("[Paxos] Accept: Connecting to node: %s...", node)
		client := connectToNode(node)

		p.logger.Infof("[Paxos] Calling Accept() on node: %s...", node)
		resp, err := client.Accept(context.Background(), &paxosApi.AcceptRequest{
			N:    n,
			View: v,
		})

		if err != nil {
			p.logger.Errorf("[Paxos] Error during ACCEPT for node %s: %v.", node, err)
			continue
		}

		if resp.Success {
			p.logger.Infof("[Paxos] Received ACCEPT-SUCCESS for node %s.", node)
			acceptOk++
		}
	}

	p.logger.Infof("[Paxos] Accepted accept-ok (%d) with majority (%d).", acceptOk, majority)

	if acceptOk < majority {
		p.logger.Infof("[Paxos] Prepare (%d) rejected, not enough votes for majority (received %d, required %d).", vId, acceptOk, majority)
		return vId, vOld
	}

	for _, node := range v {
		p.logger.Infof("[Paxos] Decide: Connecting to node: %s...", node)
		client := connectToNode(node)

		p.logger.Infof("[Paxos] Calling Decide() on node: %s...", node)
		_, err := client.Decide(context.Background(), &paxosApi.DecideRequest{
			ViewId: vId,
			View:   v,
		})

		if err != nil {
			p.logger.Errorf("[Paxos] Error during DECIDE for node %s: %v.", node, err)
			continue
		}
	}

	p.logger.Errorf("[Paxos] New value proposed. View ID: %d, Proposed values: %v", vId, v)

	return vId, v
}

func (p *Proposer) nextProposalNumber() int64 {
	p.proposalCounter++
	return (p.proposalCounter << 16) | p.nodeIdHash
}