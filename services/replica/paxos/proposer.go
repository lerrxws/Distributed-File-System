package paxos

import (
	"context"
	"time"

	paxosApi "dfs/proto-gen/paxos"

	"github.com/cihub/seelog"
)

type Proposer struct {
	commitView CommitViewFunc

	logger seelog.LoggerInterface
}

func NewProposer(commitView CommitViewFunc, logger seelog.LoggerInterface) *Proposer {
	return &Proposer{
		commitView: commitView,
		logger:  logger,
	}
}

func (p *Proposer) Propose(viewId int64, oldView []string, newView []string) {
	majority := len(oldView)/2 + 1

	clients := connectToAllPaxosClients(oldView)

	n := p.generateUniqueProposalNumber()

	p.logger.Infof("[Paxos] Starting Prepare phase with proposal number %d.", n)

	prepareResponses := p.sendPrepareRequestToAcceptors(clients, viewId, n)

	if p.hasOldInstanceResponse(prepareResponses) {
		p.logger.Infof("[Paxos] Received OLDINSTANCE.")
		p.commitToLocalInstance(prepareResponses)
		return
	}

	if !p.isPrepareOkMajority(majority, prepareResponses) {
		p.logger.Infof("[Paxos] Prepare (%d) rejected, not enough votes for majority.", n)
		return
	}

	proposedView := p.decideOnViewValue(prepareResponses, viewId, newView)

	p.logger.Infof("[Paxos] Starting Accept phase with proposal number %d.", n)

	acceptResponses := p.sendAcceptRequestToAcceptors(clients, n, proposedView)
	if !p.isAcceptOkMajority(majority, acceptResponses) {
		p.logger.Infof("[Paxos] Accept (%d) rejected, not enough votes for majority.", n)
		return
	}

	p.logger.Infof("[Paxos] Starting Decide phase with proposal number %d.", n)

	p.sendDecideRequestToAcceptors(clients, viewId, proposedView)
}

func (p *Proposer) decideOnViewValue(
	responses []*paxosApi.PrepareResponse,
	ownViewId int64,
	ownView []string,
) []string {

	var (
		maxNA     int64 = -1
		chosenView      []string
	)

	for _, resp := range responses {
		if resp.ViewId > maxNA && resp.View != nil {
			maxNA = resp.ViewId
			chosenView = resp.View
		}
	}

	if maxNA == -1 {
		return ownView
	}

	return chosenView
}


// region help functions

// region prepare

func (p *Proposer) generateUniqueProposalNumber() int64 {
	proposalNumber := time.Now().UnixNano()
	return proposalNumber
}

func (p *Proposer) sendPrepareRequestToAcceptors(clients []paxosApi.PaxosServiceClient, viewId int64, n int64) []*paxosApi.PrepareResponse {
	responses := []*paxosApi.PrepareResponse{}
	for _, client := range clients {
		resp, err := client.Prepare(context.Background(), &paxosApi.PrepareRequest{
			ViewId: viewId,
			N:      n,
		})

		if err != nil {
			p.logger.Errorf("[Paxos] Error during PREPARE for node %s: %v.", client, err)
			continue
		}

		responses = append(responses, resp)
	}

	return responses
}

func (p *Proposer) hasOldInstanceResponse(prepareResponses []*paxosApi.PrepareResponse) bool {
	for _, response := range prepareResponses {
		if response.IsOld {
			return true
		}
	}

	return false
}

func (p *Proposer) commitToLocalInstance(prepareResponses []*paxosApi.PrepareResponse) {
	for _, response := range prepareResponses {
		if response.IsOld {
			p.commitView(response.ViewId, response.View)
		}
	}
}

func (p *Proposer) isPrepareOkMajority(majority int, prepareResponses []*paxosApi.PrepareResponse) bool {
	positiveResp := 0
	for _, response := range prepareResponses {
		if response.Success {
			positiveResp++
		}
	}

	return positiveResp >= majority
}

// endregion

// region accept

func (p *Proposer) sendAcceptRequestToAcceptors(clients []paxosApi.PaxosServiceClient, n int64, newView []string) []*paxosApi.AcceptResponse {
	responses := []*paxosApi.AcceptResponse{}
	for _, client := range clients {
		resp, err := client.Accept(context.Background(), &paxosApi.AcceptRequest{
			N:    n,
			View: newView,
		})

		if err != nil {
			p.logger.Errorf("[Paxos] Error during Accept for node %s: %v.", client, err)
			continue
		}

		responses = append(responses, resp)
	}

	return responses
}

func (p *Proposer) isAcceptOkMajority(majority int, acceptResponses []*paxosApi.AcceptResponse) bool {

	positiveResp := 0
	for _, response := range acceptResponses {
		if response.Success {
			positiveResp++
		}
	}

	return positiveResp >= majority
}

// endregion

// refion decide

func (p *Proposer) sendDecideRequestToAcceptors(clients []paxosApi.PaxosServiceClient, viewId int64, newView []string) {
	for _, client := range clients {
		_, err := client.Decide(context.Background(), &paxosApi.DecideRequest{
			ViewId: viewId,
			View:   newView,
		})

		if err != nil {
			p.logger.Errorf("[Paxos] Error during Accept for node %s: %v.", client, err)
			continue
		}
	}
}

// endregion

// endregion
