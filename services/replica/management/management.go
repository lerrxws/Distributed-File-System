package management

import (
	"context"
	"time"

	utils "dfs/utils"

	managementApi "dfs/proto-gen/management"
	replicaApi "dfs/proto-gen/replica"

	paxos "dfs/services/replica/paxos"

	fl "dfs/utils/filelogger"
	seelog "github.com/cihub/seelog"
)

type ViewManager struct {
	IsPrimary bool
	Addr      string

	ViewId int64
	View   []string

	Proposer paxos.Proposer
	Acceptor paxos.Acceptor

	heartbeatInterval time.Duration
	logger            seelog.LoggerInterface
}

func NewViewManager(
	addr string, 
	heartbeatIntervalInSeconds int, 
	logger seelog.LoggerInterface, 
	filelogger *fl.JsonFileLogger,
) *ViewManager {
	m := &ViewManager{
		Addr:              addr,
		View:              []string{addr},
		ViewId:            1,
		heartbeatInterval: time.Duration(heartbeatIntervalInSeconds) * time.Second,

		logger: logger,
	}

	m.Acceptor = *paxos.NewAcceptor(m.ViewId, m.View, m.CommitView, m.logger, filelogger)
	m.Proposer = *paxos.NewProposer(m.CommitView, logger)

	return m
}

func (m *ViewManager) MonitorViewHealth() {
	ticker := time.NewTicker(m.heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		m.logger.Infof("[Management] Starting new heartbeat cycle for view: %v", m.View)

		for _, node := range m.View {
			if node == m.Addr {
				continue
			}
			
			m.sendHeartbeat(node)
		}
	}
}

func (m *ViewManager) sendHeartbeat(node string) {
	client, err := utils.ConnectToManagementClient(node)
	if err != nil {
		m.removeFromView(node)
	}

	m.logger.Infof("[Management] Attempting to send heartbeat to node: %s", node)

	_, err = client.Heartbeat(context.Background(), &managementApi.HeartbeatRequest{
		Sender: m.Addr,
	})

	if err != nil {
		m.logger.Errorf("[Management] Failed to connect to node %s: %v", node, err)

		m.removeFromView(node)
	}
}

func (m *ViewManager) CommitView(viewId int64, view []string) {
	m.logger.Infof("[Management] Commiting new view (id: %d, value: %s).", viewId, view)
	m.ViewId = viewId
	m.View = view

	if m.IsNodePrimary() {
		m.logger.Infof("[Management] This node is now the primary node: %s", m.Addr)
		m.IsPrimary = true
	}
}

func (m *ViewManager) JoinView(primaryAddr string) {
	m.logger.Infof("[Management] Attempting to connect to primary node %s.", primaryAddr)

	replicaClient, err := utils.ConnectToReplicaClient(primaryAddr)
	if err != nil {
		m.logger.Infof("[Management] Error while connecting to primary node %s. Becoming the primary node.", primaryAddr)
		m.IsPrimary = true
		return
	}

	m.logger.Infof("[Management] Attempting to synchronize view with primary node %s.", primaryAddr)
	resp, err := replicaClient.GetView(context.Background(), &replicaApi.GetViewRequest{})
	if err != nil {
		m.logger.Infof("[Management] Error while synchronizing view with primary node %s. Becoming the primary node.", primaryAddr)
		m.IsPrimary = true
		return
	}

	m.logger.Infof("[Management] View synchronized with primary node %s. Attempting to add node %s to the view.", primaryAddr, m.Addr)

	newViewId := resp.ViewId + 1
	newView := append(resp.View, m.Addr)

	m.logger.Infof("[Management] Proposing new view with ViewId %d: %v", newViewId, newView)
	go m.Proposer.Propose(newViewId, resp.View, newView)
}

func (m *ViewManager) removeFromView(nodeAddr string) {
	m.logger.Infof("[Management] Attempting to remove node %s from the view.", nodeAddr)

	newViewId := m.ViewId + 1
	newView := []string{}

	for _, node := range m.View {
		if node != nodeAddr {
			newView = append(newView, node)
		}
	}

	m.logger.Infof("[Management] Node %s removed from the view. New view: %v", nodeAddr, newView)

	m.logger.Infof("[Management] Proposing new view with ViewId %d: %v", newViewId, newView)

	go m.Proposer.Propose(newViewId, newView, newView)
}

func (m *ViewManager) IsNodePrimary() bool {
	return m.View[0] == m.Addr
}

func (m *ViewManager) GetPrimaryNodeAddr() string {
	if len(m.View) < 1 {
		m.logger.Errorf("[Management] Error: The view is empty. Cannot proceed with operation. The system is in an invalid state.")
		return ""
	}

	if m.IsPrimary {
		return m.Addr
	}

	return m.View[0]
}

func (m *ViewManager) GetViewWithoutOwnAddr() []string {
	newView := []string{}

	for _, node := range m.View {
		if node != m.Addr {
			newView = append(newView, node)
		}
	}

	return newView
}
