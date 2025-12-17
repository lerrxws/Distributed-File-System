package replica

import (
	"context"
	"fmt"
	// "sync"
	"time"

	managementApi "dfs/proto-gen/management"
	replicaApi "dfs/proto-gen/replica"
	utils "dfs/utils"

	"github.com/cihub/seelog"
)

type ManagementModule struct {
	logger seelog.LoggerInterface

	stopChan chan struct{}

	recoverer *RecoveryModule
	paxos     *PaxosModule

	// Callbacks to interact with replica state
	getAddr        func() string
	getView        func() (int64, []string)
	getIsPrimary   func() bool
	setIsPrimary   func(bool)
	setIsRecovered func(bool)
	commitView     func(viewId int64, view []string)
}

func NewViewManager(
	logger seelog.LoggerInterface,

	recoverer *RecoveryModule,
	paxos *PaxosModule,

	getAddr func() string,
	getView func() (int64, []string),
	getIsPrimary func() bool,
	setIsPrimary func(bool),
	setIsRecovered func(bool),
	commitView func(viewId int64, view []string),
) *ManagementModule {
	return &ManagementModule{
		logger: logger,

		stopChan: make(chan struct{}),

		recoverer: recoverer,
		paxos:     paxos,

		getAddr:        getAddr,
		getView:        getView,
		getIsPrimary:   getIsPrimary,
		setIsPrimary:   setIsPrimary,
		setIsRecovered: setIsRecovered,
		commitView:     commitView,
	}
}

func (mm *ManagementModule) JoinToView(primaryAddr string) error {
	mm.logger.Infof("[Management] Attempting to join the view from primary node %s", primaryAddr)

	viewId, view, err := mm.fetchViewFromPrimary(primaryAddr)
	if err != nil {
		return mm.logger.Errorf("failed to get view from primary node %s: %v", primaryAddr, err)
	}

	mm.logger.Infof("[Management] Retrieved view from primary node %s: viewId = %d, view = %v", primaryAddr, viewId, view)

	newViewId := viewId + 1
	newView := append(view, mm.getAddr())

	commit, decidedViewId, decidedView := mm.initiateViewChange(newViewId, view, newView)
	if !commit {
		mm.setIsPrimary(true)
		return mm.logger.Errorf("[Management] Unable to join view %s.", view)
	}

	mm.commitView(decidedViewId, decidedView)

	primaryAddr = decidedView[0]
	isPrimary := (primaryAddr == mm.getAddr())
	if isPrimary {
		mm.setIsPrimary(true)
	}

	// 4. Recovery БЕЗ лока
	mm.logger.Infof("[ManagementModule] Starting recovery from primary %s", primaryAddr)
	mm.setIsRecovered(false)
	if err := mm.recoverer.RecoverFromPrimary(primaryAddr, isPrimary); err != nil {
		mm.logger.Errorf("[ManagementModule] Recovery from primary %s failed: %v", primaryAddr, err)
	}
	mm.setIsRecovered(true)

	return nil
}

func (mm *ManagementModule) StartHealthMonitoring(interval time.Duration) {
	go func() {
		mm.logger.Infof("[ManagementModule] Started monitoring view health every %v.", interval)

		for {
			select {
			case <-time.After(interval * time.Second):
				mm.checkAndRemoveDeadReplicas()
			case <-mm.stopChan:
				mm.logger.Infof("[ManagementModule] Health monitoring stopped.")
				return
			}
		}
	}()
}

// region VIEW CHANGES - Proposing and Coordinating

func (mm *ManagementModule) initiateViewChange(newViewId int64, oldView []string, newView []string) (bool, int64, []string) {
	mm.logger.Infof("[ManagementModule] Initiating view change (newViewId: %d, newView: %v).", newViewId, newView)

	return mm.proposeView(newViewId, oldView, newView)
}

func (mm *ManagementModule) proposeView(newViewId int64, oldView []string, newView []string) (bool, int64, []string) {
	return mm.paxos.Propose(newViewId, oldView, newView)
}

func (mm *ManagementModule) removeReplicaFromView(nodeAddr string) {
	mm.logger.Infof("[ManagementModule] Removing replica %s from the view.", nodeAddr)

	viewId, view := mm.getView()
	newViewId := viewId + 1
	newView := []string{}

	for _, replica := range view {
		if replica != nodeAddr {
			newView = append(newView, replica)
		}
	}

	mm.logger.Infof("[ManagementModule] New view generated with viewId %d: %v. Proposing it.", newViewId, newView)

	mm.initiateViewChange(newViewId, view, newView)
}

// endregion

// region HEALTH CHECKING - Replica Liveness Detection

func (mm *ManagementModule) checkAndRemoveDeadReplicas() {
	_, view := mm.getView()
	myAddr := mm.getAddr()

	mm.logger.Infof("[ManagementModule] Checking health of replicas in the view: %v", view)

	for _, replica := range view {
		if replica == myAddr {
			continue
		}

		if !mm.isReplicaAlive(replica) {
			mm.logger.Infof("[ManagementModule] Replica %s is not alive, removing it from the view.", replica)

			mm.removeReplicaFromView(replica)

			return // Only remove one replica at a time
		}
	}
}

// isReplicaAlive checks if a replica is responsive by sending a heartbeat request.
func (mm *ManagementModule) isReplicaAlive(replicaAddr string) bool {
	replica, err := utils.ConnectToManagementClient(replicaAddr)
	if err != nil {
		mm.logger.Errorf("[ManagementModule] Failed to connect to replica %s: %v", replicaAddr, err)
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = replica.Heartbeat(ctx, &managementApi.HeartbeatRequest{Sender: mm.getAddr()})
	if err != nil {
		mm.logger.Errorf("[ManagementModule] Failed to receive heartbeat from replica %s: %v", replicaAddr, err)
		return false
	}

	mm.logger.Infof("[ManagementModule] Replica %s is alive.", replicaAddr)
	return true
}

// endregion

// region VIEW QUERIES - Fetching Remote View State

func (mm *ManagementModule) fetchViewFromPrimary(primaryAddr string) (int64, []string, error) {
	primary, err := utils.ConnectToReplicaClient(primaryAddr)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to connect to primary node: %v", err)
	}

	view, err := primary.GetView(context.Background(), &replicaApi.GetViewRequest{})
	if err != nil {
		return 0, nil, fmt.Errorf("failed to get view from primary node: %v", err)
	}

	mm.logger.Infof(
		"[ManagementModule] Successfully retrieved view from primary node %s: viewId = %d, view = %v",
		primaryAddr, view.ViewId, view.View,
	)

	return view.ViewId, view.View, nil
}

// endregion

// region stop

func (mm *ManagementModule) StopHealthMonitoring() {
	close(mm.stopChan)
}

// endregion
