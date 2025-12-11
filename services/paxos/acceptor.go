package paxos

import (
	paxosApi "dfs/proto-gen/paxos"

	seelog "github.com/cihub/seelog"
)

type Acceptor struct {
	viewId_h int64
	n_h      int64
	n_a      int64
	v_a      []string

	logger seelog.LoggerInterface
}

func NewAcceptor(initialViewId int64, initialView []string, logger seelog.LoggerInterface) *Acceptor{
	return &Acceptor{
		viewId_h: initialViewId,
		v_a:      initialView,

		logger: logger,
	}
}

func (a *Acceptor) Prepare(req *paxosApi.PrepareRequest) (*paxosApi.PrepareResponse, error) {
	a.logger.Infof("[Paxos] Prepare request with propose number - %d and view ID - %d.", req.N, req.ViewId)

	if req.ViewId <= a.viewId_h {
		a.logger.Infof("[Paxos] This viewId is older or equal to current viewId_h=%d. Returning previous view.", req.ViewId)

		return a.handleOldInstance(req.ViewId)
	} else if req.N > a.n_h {
		a.logger.Infof("[Paxos] Sending prepare-ok for propose %d", req.N) 

		a.n_h = req.N

		return a.handlePrepareOk(), nil
	}

	a.logger.Infof("[Paxos] Sending prepare-reject for propose %d", req.N) 

	return a.handlePrepareReject(), nil
}

func (a *Acceptor) Accept(req *paxosApi.AcceptRequest) (*paxosApi.AcceptResponse, error) {
	a.logger.Infof("[Paxos] Access request with propose number - %d and view - %d.", req.N, req.View)

	if req.N >= a.n_h {
		a.logger.Infof("[Paxos] Sending access-ok for propose %d", req.N)

		a.n_a = req.N
		a.v_a = req.View

		return a.handleAcceptOk(), nil
	}

	a.logger.Infof("[Paxos] Sending access-reject for propose %d", req.N)

	return a.handleAcceptReject(), nil
}

func (a *Acceptor) Decide(req *paxosApi.DecideRequest) (*paxosApi.DecideResponse, error) {
	a.logger.Infof("[Paxos] Decide request with propose number - %d and view ID - %d.", req.ViewId)

	if req.ViewId > a.viewId_h {
		a.logger.Infof("[Paxos] Decide request with propose number - %d and view ID - %d.", req.ViewId)

		a.viewId_h = req.ViewId

		a.commitView()
	}

	return  &paxosApi.DecideResponse{}, nil
}

// region help functions

// region response-handler

func (a *Acceptor) handleOldInstance(vId int64) (*paxosApi.PrepareResponse, error) {
	return &paxosApi.PrepareResponse{
		Success: false,
		IsOld:   true,
		ViewId:  a.viewId_h,
		View:    a.v_a,
	}, nil
}

func (a *Acceptor) handlePrepareOk() *paxosApi.PrepareResponse {
	return &paxosApi.PrepareResponse{
		Success: true,
		IsOld:   false,
		ViewId:  a.n_a,
		View:    a.v_a,
	}
}

func (a *Acceptor) handlePrepareReject() *paxosApi.PrepareResponse {
	return &paxosApi.PrepareResponse{
		Success: false,
		IsOld:   false,
		ViewId:  a.n_a,
		View:    a.v_a,
	}
}

func (a *Acceptor) handleAcceptOk() *paxosApi.AcceptResponse {
	return &paxosApi.AcceptResponse{
		Success: true,
	}
}

func (a *Acceptor) handleAcceptReject() *paxosApi.AcceptResponse {
	return &paxosApi.AcceptResponse{
		Success: false,
	}
}

// endregion

func (a *Acceptor) commitView(){
	// TODO
}

// endregion