package replica

import (
	"context"
	"sync"
	"time"

	paxosApi "dfs/proto-gen/paxos"
	utils "dfs/utils"

	fl "dfs/utils/filelogger"

	"github.com/cihub/seelog"
)

type PaxosModule struct {
	viewId int64
	n_h      int64
	n_a      int64
	v_a      []string

	commitView func(viewId int64, view []string)

	filelogger *fl.JsonFileLogger
	logger seelog.LoggerInterface

	mu sync.Mutex
}

func NewPaxosModule(
	commitView func(viewId int64, view []string),

	logger seelog.LoggerInterface, 
	filelogger *fl.JsonFileLogger,
) *PaxosModule{
	pm := &PaxosModule{
		viewId: 0,
		n_h: 0,
		n_a: 0,
		v_a: nil,

		commitView: commitView,

		logger: logger,
		filelogger: filelogger,
	}

	return pm
}

func (pm *PaxosModule) Propose(viewId int64, oldView []string, newView []string) (bool, int64, []string) {
	majority := len(oldView)/2 + 1

	clients := connectToAllPaxosClients(oldView)

	n := pm.generateUniqueProposalNumber()

	pm.logger.Infof("[Paxos] Majority: %d.", majority)
	pm.logger.Infof("[Paxos] Starting Prepare phase with proposal number %d.", n)

	prepareResponses := pm.sendPrepareRequestToAcceptors(clients, viewId, n)

	if isOld, actualViewId, actualView := pm.hasOldInstanceResponse(prepareResponses); isOld {
		pm.logger.Infof("[Paxos] Received OLDINSTANCE.")
		// pm.commitToLocalInstance(prepareResponses)
		return true, actualViewId, actualView
	}

	if !pm.isPrepareOkMajority(majority, prepareResponses) {
		pm.logger.Infof("[Paxos] Prepare (%d) rejected, not enough votes for majority.", n)
		return false, -1, nil
	}

	proposedView := pm.decideOnViewValue(prepareResponses, n, newView)

	pm.logger.Infof("[Paxos] Starting Accept phase with proposal number %d (view: %s).", n, proposedView)

	acceptResponses := pm.sendAcceptRequestToAcceptors(clients, n, proposedView)
	if !pm.isAcceptOkMajority(majority, acceptResponses) {
		pm.logger.Infof("[Paxos] Accept (%d) rejected, not enough votes for majority.", n)
		return false, -1, nil
	}

	pm.logger.Infof("[Paxos] Starting Decide phase with proposal number %d.", n)

	pm.sendDecideRequestToAcceptors(clients, viewId, proposedView)
	// pm.commitView(viewId, proposedView)

	return true, viewId, proposedView
}

func (pm *PaxosModule) Prepare(req *paxosApi.PrepareRequest) (*paxosApi.PrepareResponse, error) {
	pm.mu.Lock()
    defer pm.mu.Unlock()

	pm.logger.Infof("[Paxos] Prepare request with propose number - %d and view ID - %d.", req.N, req.ViewId)

	if req.ViewId <= pm.viewId {
		pm.logger.Infof("[Paxos] This viewId is older or equal to current viewId=%d. Returning previous view.", req.ViewId)

		return pm.handleOldInstance(req.ViewId)
	} else if req.N > pm.n_h {
		pm.logger.Infof("[Paxos] Sending prepare-ok for propose %d", req.N)

		pm.n_h = req.N

		return pm.handlePrepareOk(), nil
	}

	pm.logger.Infof("[Paxos] Sending prepare-reject for propose %d", req.N)

	return pm.handlePrepareReject(), nil
}

func (pm *PaxosModule) Accept(req *paxosApi.AcceptRequest) (*paxosApi.AcceptResponse, error) {
	pm.mu.Lock()
    defer pm.mu.Unlock()

	pm.logger.Infof("[Paxos] Accept request with propose number - %d and view - %s.", req.N, req.View)

	if req.N >= pm.n_h {
		pm.logger.Infof("[Paxos] Sending accept-ok for propose %d", req.N)

		pm.n_a = req.N
		pm.v_a = req.View

		return pm.handleAcceptOk(), nil
	}

	pm.logger.Infof("[Paxos] Sending accept-reject for propose %d", req.N)

	return pm.handleAcceptReject(), nil
}

func (pm *PaxosModule) Decide(req *paxosApi.DecideRequest) (bool, error) {
	pm.mu.Lock()
    defer pm.mu.Unlock()

	pm.logger.Infof("[Paxos] Decide request with view ID - %d.", req.ViewId)

	if req.ViewId > pm.viewId {
		pm.logger.Infof("[Paxos] Decide request is accepted.")

		pm.viewId = req.ViewId

		pm.commitView(req.ViewId, req.View)

		err := pm.logToFile()
		if err != nil {
			pm.logger.Errorf(
				"[Paxos] Failed to log Paxos state (instance=%d, n_h=%d, n_a=%d, v_a=%v): %v",
				pm.viewId,
				pm.n_h,
				pm.n_a,
				pm.v_a,
				err,
			)
		}

		return true, nil
	}

	pm.logger.Infof("[Paxos] Decide request is ignored.")

	return false, nil
}

// region proposer help functions

// region prepare

func (pm *PaxosModule) generateUniqueProposalNumber() int64 {
	proposalNumber := time.Now().UnixNano()
	return proposalNumber
}

func (pm *PaxosModule) sendPrepareRequestToAcceptors(clients []paxosApi.PaxosServiceClient, viewId int64, n int64) []*paxosApi.PrepareResponse {
	responses := []*paxosApi.PrepareResponse{}
	for _, client := range clients {
		resp, err := client.Prepare(context.Background(), &paxosApi.PrepareRequest{
			ViewId: viewId,
			N:      n,
		})

		if err != nil {
			pm.logger.Errorf("[Paxos] Error during PREPARE for node %s: %v.", client, err)
			continue
		}

		responses = append(responses, resp)
	}

	return responses
}

func (pm *PaxosModule) hasOldInstanceResponse(prepareResponses []*paxosApi.PrepareResponse) (bool, int64, []string) {
	for _, response := range prepareResponses {
		if response.IsOld {
			return true, response.ViewId, response.View
		}
	}

	return false, -1, nil
}

func (pm *PaxosModule) commitToLocalInstance(prepareResponses []*paxosApi.PrepareResponse) {
	for _, response := range prepareResponses {
		if response.IsOld {
			pm.commitView(response.ViewId, response.View)
		}
	}
}

func (pm *PaxosModule) isPrepareOkMajority(majority int, prepareResponses []*paxosApi.PrepareResponse) bool {
	positiveResp := 0
	for _, response := range prepareResponses {
		if response.Success {
			positiveResp++
		}
	}

	return positiveResp >= majority
}

func (pm *PaxosModule) decideOnViewValue( responses []*paxosApi.PrepareResponse, own_n int64, ownView []string) []string {

	pm.logger.Infof("[Paxos] Decide value after Prepare: n=%d ownView=%v", own_n, ownView)

	maxNA := own_n
	chosenView := append([]string{}, ownView...)

	for _, resp := range responses {
		pm.logger.Infof("[Paxos] PrepareResponse: viewId=%d view=%v", resp.ViewId, resp.View)

		if resp.View != nil && resp.ViewId > maxNA {
			maxNA = resp.ViewId
			chosenView = append([]string{}, resp.View...)
			pm.logger.Infof("[Paxos] New candidate selected: viewId=%d view=%v", resp.ViewId, resp.View)
		}
	}

	pm.logger.Infof("[Paxos] Final chosen value: n_a=%d view=%v", maxNA, chosenView)

	return chosenView
}

// endregion

// region accept

func (pm *PaxosModule) sendAcceptRequestToAcceptors(clients []paxosApi.PaxosServiceClient, n int64, newView []string) []*paxosApi.AcceptResponse {
	responses := []*paxosApi.AcceptResponse{}
	for _, client := range clients {
		resp, err := client.Accept(context.Background(), &paxosApi.AcceptRequest{
			N:    n,
			View: newView,
		})

		if err != nil {
			pm.logger.Errorf("[Paxos] Error during Accept for node %s: %v.", client, err)
			continue
		}

		responses = append(responses, resp)
	}

	return responses
}

func (pm *PaxosModule) isAcceptOkMajority(majority int, acceptResponses []*paxosApi.AcceptResponse) bool {

	positiveResp := 0
	for _, response := range acceptResponses {
		if response.Success {
			positiveResp++
		}
	}

	return positiveResp >= majority
}

// endregion

// region decide

func (pm *PaxosModule) sendDecideRequestToAcceptors(clients []paxosApi.PaxosServiceClient, viewId int64, newView []string) {
	for _, client := range clients {
		_, err := client.Decide(context.Background(), &paxosApi.DecideRequest{
			ViewId: viewId,
			View:   newView,
		})

		if err != nil {
			pm.logger.Errorf("[Paxos] Error during Accept for node %s: %v.", client, err)
			continue
		}
	}
}

// endregion

// endregion

// region acceptor help functions

// region response-handler

func (pm *PaxosModule) handleOldInstance(vId int64) (*paxosApi.PrepareResponse, error) {
	return &paxosApi.PrepareResponse{
		Success: false,
		IsOld:   true,
		ViewId:  pm.viewId,
		View:    pm.v_a,
	}, nil
}

func (pm *PaxosModule) handlePrepareOk() *paxosApi.PrepareResponse {
	return &paxosApi.PrepareResponse{
		Success: true,
		IsOld:   false,
		ViewId:  pm.n_a,
		View:    pm.v_a,
	}
}

func (pm *PaxosModule) handlePrepareReject() *paxosApi.PrepareResponse {
	return &paxosApi.PrepareResponse{
		Success: false,
		IsOld:   false,
		ViewId:  pm.n_a,
		View:    pm.v_a,
	}
}

func (pm *PaxosModule) handleAcceptOk() *paxosApi.AcceptResponse {
	return &paxosApi.AcceptResponse{
		Success: true,
	}
}

func (pm *PaxosModule) handleAcceptReject() *paxosApi.AcceptResponse {
	return &paxosApi.AcceptResponse{
		Success: false,
	}
}

// endregion


func (pm *PaxosModule) resetViewData() {
	pm.n_a = 0
	pm.v_a = nil
}

func (pm *PaxosModule) logToFile() error{
	timestamp := utils.GetTimeStamp()

	entry := fl.PaxosLogEntry{
		Timestamp: timestamp,
		Instance: pm.viewId,
		NH: pm.n_h,
		NA: pm.n_a,
		VA: pm.v_a,	
	}

	return pm.filelogger.Append(entry)
}
// endregion
