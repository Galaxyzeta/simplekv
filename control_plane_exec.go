package simplekv

import (
	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/proto"
	"github.com/galaxyzeta/simplekv/util"
)

const controlPlaneCmdChannelSize = 1024
const (
	ctrlCmd_ControllerElection controlPlaneCmdType = iota
	ctrlCmd_LeaderElection
	ctrlCmd_SetControllerName
	ctrlCmd_SetLeaderName
	ctrlCmd_SetNodeStatus
	ctrlCmd_ReceiveCollectOffsetRequest
	ctrlCmd_ReceiveRoleChangeRequest
	ctrlCmd_TestRequest
)

func (cmdType controlPlaneCmdType) String() string {
	switch cmdType {
	case ctrlCmd_ControllerElection:
		return "ControllerElection"
	case ctrlCmd_LeaderElection:
		return "LeaderElection"
	case ctrlCmd_SetControllerName:
		return "SetControllerName"
	case ctrlCmd_SetLeaderName:
		return "SetLeaderName"
	case ctrlCmd_SetNodeStatus:
		return "SetNodeStatus"
	case ctrlCmd_ReceiveCollectOffsetRequest:
		return "ReceiveCollectOffsetRequest"
	case ctrlCmd_ReceiveRoleChangeRequest:
		return "ReceiveRoleChangeRequest"
	case ctrlCmd_TestRequest:
		return "TestRequest"
	}
	return "<unknown>"
}

type controlPlaneCmdType uint8
type controlPlaneCmdWrapper struct {
	cmdtype    controlPlaneCmdType
	payload    interface{}
	completeCh chan struct{}
}

type controlPlaneExecutor struct {
	logger *util.Logger
}

func newControlPlaneExecutor() *controlPlaneExecutor {
	return &controlPlaneExecutor{
		logger: util.NewLogger("[CtrlExecutor]", config.LogOutputWriter),
	}
}

var _ctrlCmdPipe = make(chan controlPlaneCmdWrapper, controlPlaneCmdChannelSize)

// --- Operators and Payloads ---

type cmdPayload_SetNodeStatus struct {
	nodeName  string
	isOffline bool
}
type cmdPayload_ReceiveRoleChangeRequest struct {
	role   proto.ServerRole
	result *bool
}

// --- Enqueue Operations ---

func (ce *controlPlaneExecutor) enqueueCmd(cmdtype controlPlaneCmdType, payload interface{}) <-chan struct{} {
	ch := make(chan struct{}, 1)
	_ctrlCmdPipe <- controlPlaneCmdWrapper{
		cmdtype:    cmdtype,
		payload:    payload,
		completeCh: ch,
	}
	return (<-chan struct{})(ch)
}
func (ce *controlPlaneExecutor) enqueueAndWait_ControllerElectionCmd() {
	<-ce.enqueueCmd(ctrlCmd_ControllerElection, nil)
}
func (ce *controlPlaneExecutor) enqueueAndWait_LeaderElectionCmd() {
	<-ce.enqueueCmd(ctrlCmd_LeaderElection, nil)
}
func (ce *controlPlaneExecutor) enqueueAndWait_SetControllerNameCmd(name string) {
	<-ce.enqueueCmd(ctrlCmd_SetControllerName, name)
}
func (ce *controlPlaneExecutor) enqueueAndWait_SetLeaderNameCmd(leaderData string) {
	<-ce.enqueueCmd(ctrlCmd_SetLeaderName, leaderData)
}
func (ce *controlPlaneExecutor) enqueueAndWait_SetNodeStatus(nodeName string, isOffline bool) {
	<-ce.enqueueCmd(ctrlCmd_SetNodeStatus, cmdPayload_SetNodeStatus{
		nodeName:  nodeName,
		isOffline: isOffline,
	})
}
func (ce *controlPlaneExecutor) enqueueAndWait_ReceiveCollectOffsetRequest() {
	<-ce.enqueueCmd(ctrlCmd_ReceiveCollectOffsetRequest, nil)
}
func (ce *controlPlaneExecutor) enqueueAndWait_ReceiveRoleChangeRequest(role proto.ServerRole, result *bool) {
	<-ce.enqueueCmd(ctrlCmd_ReceiveRoleChangeRequest, cmdPayload_ReceiveRoleChangeRequest{
		role:   role,
		result: result,
	})
}

// Test only
func (ce *controlPlaneExecutor) enqueueAndWait_TestRequest(fn func()) {
	<-ce.enqueueCmd(ctrlCmd_TestRequest, fn)
}

// --- Handlers ---

func (ce *controlPlaneExecutor) handleControllerElection(wrapper controlPlaneCmdWrapper) {
	ctrlInstance.controllerElectionMgr.runOnce()
	wrapper.completeCh <- struct{}{}
}
func (ce *controlPlaneExecutor) handleLeaderElection(wrapper controlPlaneCmdWrapper) {
	ctrlInstance.leaderElectionMgr.runElectionOnce()
	wrapper.completeCh <- struct{}{}
}
func (ce *controlPlaneExecutor) handleSetControllerName(wrapper controlPlaneCmdWrapper) {
	ctrlInstance.setCurrentControllerName(wrapper.payload.(string))
	wrapper.completeCh <- struct{}{}
}
func (ce *controlPlaneExecutor) handleSetLeaderName(wrapper controlPlaneCmdWrapper) {
	leaderName := util.ParseZkLeader(wrapper.payload.(string)).Name
	ce.logger.Infof("Setting leader name = %s", leaderName)
	ctrlInstance.setCurrentLeaderName(leaderName)
	wrapper.completeCh <- struct{}{}
}
func (ce *controlPlaneExecutor) handleSetNodeStatus(wrapper controlPlaneCmdWrapper) {
	param := wrapper.payload.(cmdPayload_SetNodeStatus)
	ce.logger.Infof("Setting node %s, isOffline = %t", param.nodeName, param.isOffline)
	if param.isOffline {
		ctrlInstance.addToOffline(param.nodeName)
	} else {
		ctrlInstance.removeFromOffline(param.nodeName)
	}
	wrapper.completeCh <- struct{}{}
}
func (ce *controlPlaneExecutor) handleReceiveOffsetCollectRequest(wrapper controlPlaneCmdWrapper) {
	ctrlInstance.leaderElectionMgr.handleReceiveOffsetCollectRequest()
	wrapper.completeCh <- struct{}{}
}
func (ce *controlPlaneExecutor) handleReceiveRoleChangeRequest(wrapper controlPlaneCmdWrapper) {
	param := wrapper.payload.(cmdPayload_ReceiveRoleChangeRequest)
	ce.logger.Infof("Setting role to %s", param.role.String())
	rs := ctrlInstance.leaderElectionMgr.handleReceiveRoleChangeRequest(param.role)
	*param.result = rs
	wrapper.completeCh <- struct{}{}
}
func (ce *controlPlaneExecutor) handleTestRequest(wrapper controlPlaneCmdWrapper) {
	fn := wrapper.payload.(func())
	fn()
	wrapper.completeCh <- struct{}{}
}

func (ce *controlPlaneExecutor) run() {
	for {
		cmdWrapper := <-_ctrlCmdPipe
		ce.logger.Infof("Handling cmd type = %s ...", cmdWrapper.cmdtype.String())
		switch cmdWrapper.cmdtype {
		case ctrlCmd_ControllerElection:
			ce.handleControllerElection(cmdWrapper)
		case ctrlCmd_LeaderElection:
			ce.handleLeaderElection(cmdWrapper)
		case ctrlCmd_SetControllerName:
			ce.handleSetControllerName(cmdWrapper)
		case ctrlCmd_SetLeaderName:
			ce.handleSetLeaderName(cmdWrapper)
		case ctrlCmd_SetNodeStatus:
			ce.handleSetNodeStatus(cmdWrapper)
		case ctrlCmd_ReceiveCollectOffsetRequest:
			ce.handleReceiveOffsetCollectRequest(cmdWrapper)
		case ctrlCmd_ReceiveRoleChangeRequest:
			ce.handleReceiveRoleChangeRequest(cmdWrapper)
		case ctrlCmd_TestRequest:
			ce.handleTestRequest(cmdWrapper)
		}
	}
}
