package paxos

import (
	// "sync"
	
	paxosApi "dfs/proto-gen/paxos"

	fl "dfs/utils/filelogger"

	seelog "github.com/cihub/seelog"
)

type Acceptor struct {
	// mu sync.Mutex
	
	viewId int64
	n_h      int64
	n_a      int64
	v_a      []string

	commitView CommitViewFunc

	filelogger *fl.JsonFileLogger
	logger     seelog.LoggerInterface
}

func NewAcceptor(
	initialViewId int64, 
	initialView []string, 
	commitView CommitViewFunc, 
	logger seelog.LoggerInterface, 
	filelogger *fl.JsonFileLogger,
) *Acceptor {
	return &Acceptor{
		viewId: initialViewId,
		v_a:      initialView,

		commitView: commitView,

		logger: logger,
		filelogger: filelogger,
	}
}

func (a *Acceptor) Prepare(req *paxosApi.PrepareRequest) (*paxosApi.PrepareResponse, error) {
	a.logger.Infof("[Paxos] Prepare request with propose number - %d and view ID - %d.", req.N, req.ViewId)

	if req.ViewId <= a.viewId {
		a.logger.Infof("[Paxos] This viewId is older or equal to current viewId=%d. Returning previous view.", req.ViewId)

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
	a.logger.Infof("[Paxos] Accept request with propose number - %d and view - %s.", req.N, req.View)

	if req.N >= a.n_h {
		a.logger.Infof("[Paxos] Sending accept-ok for propose %d", req.N)

		a.n_a = req.N
		a.v_a = req.View

		return a.handleAcceptOk(), nil
	}

	a.logger.Infof("[Paxos] Sending accept-reject for propose %d", req.N)

	return a.handleAcceptReject(), nil
}

func (a *Acceptor) Decide(req *paxosApi.DecideRequest) (bool, error) {
	a.logger.Infof("[Paxos] Decide request with view ID - %d.", req.ViewId)

	if req.ViewId > a.viewId {
		a.logger.Infof("[Paxos] Decide request is accepted.")

		a.viewId = req.ViewId

		a.commitView(req.ViewId, req.View)

		err := a.logToFile()
		if err != nil {
			a.logger.Errorf(
				"[Paxos] Failed to log Paxos state (instance=%d, n_h=%d, n_a=%d, v_a=%v): %v",
				a.viewId,
				a.n_h,
				a.n_a,
				a.v_a,
				err,
			)
		}

		a.resetViewData()

		return true, nil
	}

	a.logger.Infof("[Paxos] Decide request is ignored.")

	return false, nil
}

// region help functions

// region response-handler

func (a *Acceptor) handleOldInstance(vId int64) (*paxosApi.PrepareResponse, error) {
	return &paxosApi.PrepareResponse{
		Success: false,
		IsOld:   true,
		ViewId:  a.viewId,
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


func (a *Acceptor) resetViewData() {
	a.n_a = 0
	a.v_a = nil
}
// endregion
