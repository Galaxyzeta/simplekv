package simplekv

import (
	"fmt"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/proto"
	"github.com/galaxyzeta/simplekv/util"
)

const controlPlaneCmdChannelSize = 1024
const (
	ctrlCmd_Unknown = iota
	ctrlCmd_TestRequest
	ctrlCmd_LeaderElection
	ctrlCmd_NodeConnectionChanged
	ctrlCmd_AlterIsr
	// ctrlCmd_SetLeaderName
	// ctrlCmd_SetNodeStatus

	// ctrlCmd_ReceiveCollectOffsetRequest
	// ctrlCmd_ReceiveRoleChangeRequest
	ctrlCmd_Shutdown
)

func (cmdType controlPlaneCmdType) String() string {
	switch cmdType {
	case ctrlCmd_LeaderElection:
		return "LeaderElection"
	case ctrlCmd_NodeConnectionChanged:
		return "NodeConnectionChanged"
	case ctrlCmd_AlterIsr:
		return "AlterIsr"
	// case ctrlCmd_SetLeaderName:
	// 	return "SetLeaderName"
	// case ctrlCmd_SetNodeStatus:
	// 	return "SetNodeStatus"
	// case ctrlCmd_ReceiveCollectOffsetRequest:
	// 	return "ReceiveCollectOffsetRequest"
	// case ctrlCmd_ReceiveRoleChangeRequest:
	// 	return "ReceiveRoleChangeRequest"
	case ctrlCmd_TestRequest:
		return "TestRequest"
	case ctrlCmd_Shutdown:
		return "Shutdown"
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
	logger      *util.Logger
	ctrlCmdPipe chan controlPlaneCmdWrapper
	isClosed    bool
}

func newControlPlaneExecutor() *controlPlaneExecutor {
	return &controlPlaneExecutor{
		logger:      util.NewLogger("[CtrlExecutor]", config.LogOutputWriter, config.EnableDebug),
		ctrlCmdPipe: make(chan controlPlaneCmdWrapper, controlPlaneCmdChannelSize),
	}
}

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

func (ce *controlPlaneExecutor) enqueueCmd(cmdtype controlPlaneCmdType, payload interface{}) (ch <-chan struct{}, err error) {
	defer func() {
		if k := recover(); k != nil {
			err = fmt.Errorf("%s", k)
		}
	}()
	tmpch := make(chan struct{}, 1)
	ce.ctrlCmdPipe <- controlPlaneCmdWrapper{
		cmdtype:    cmdtype,
		payload:    payload,
		completeCh: tmpch,
	}
	ch = (<-chan struct{})(tmpch)
	ce.logger.Debugf("Enqueued event: %s", cmdtype)
	return
}

func (ce *controlPlaneExecutor) enqueueNodeConnectionChanged() error {
	_, err := ce.enqueueCmd(ctrlCmd_NodeConnectionChanged, nil)
	return err
}
func (ce *controlPlaneExecutor) enqueueLeaderElection() error {
	_, err := ce.enqueueCmd(ctrlCmd_LeaderElection, nil)
	return err
}
func (ce *controlPlaneExecutor) enqueueTryAlterIsr() error {
	_, err := ce.enqueueCmd(ctrlCmd_AlterIsr, nil)
	return err
}
func (ce *controlPlaneExecutor) enqueueShutdownAndWait() error {
	wait := make(chan struct{}, 1)
	ch, err := ce.enqueueCmd(ctrlCmd_Shutdown, (chan<- struct{})(wait))
	if err != nil {
		return err
	}
	<-ch
	return nil
}

// func (ce *controlPlaneExecutor) enqueueAndWait_LeaderElectionCmd() {
// 	<-ce.enqueueCmd(ctrlCmd_LeaderElection, nil)
// }

// func (ce *controlPlaneExecutor) enqueueAndWait_SetLeaderNameCmd(leaderData string) {
// 	<-ce.enqueueCmd(ctrlCmd_SetLeaderName, leaderData)
// }
// func (ce *controlPlaneExecutor) enqueueAndWait_SetNodeStatus(nodeName string, isOffline bool) {
// 	<-ce.enqueueCmd(ctrlCmd_SetNodeStatus, cmdPayload_SetNodeStatus{
// 		nodeName:  nodeName,
// 		isOffline: isOffline,
// 	})
// }
// func (ce *controlPlaneExecutor) enqueueAndWait_ReceiveCollectOffsetRequest() {
// 	<-ce.enqueueCmd(ctrlCmd_ReceiveCollectOffsetRequest, nil)
// }
// func (ce *controlPlaneExecutor) enqueueAndWait_ReceiveRoleChangeRequest(role proto.ServerRole, result *bool) {
// 	<-ce.enqueueCmd(ctrlCmd_ReceiveRoleChangeRequest, cmdPayload_ReceiveRoleChangeRequest{
// 		role:   role,
// 		result: result,
// 	})
// }

// Test only
func (ce *controlPlaneExecutor) enqueueAndWait_TestRequest(fn func()) {
	ch, err := ce.enqueueCmd(ctrlCmd_TestRequest, fn)
	if err != nil {
		panic(err)
	}
	<-ch
}

// --- processrs ---
func (ce *controlPlaneExecutor) processNodeConnectionChanged(wrapper controlPlaneCmdWrapper) {
	ctrlInstance.leaderElectionMgr.processNodeConnectionChanged()
	wrapper.completeCh <- struct{}{}
}
func (ce *controlPlaneExecutor) processLeaderElection(wrapper controlPlaneCmdWrapper) {
	ctrlInstance.leaderElectionMgr.processElect()
	wrapper.completeCh <- struct{}{}
}
func (ce *controlPlaneExecutor) processTryAlterIsr(wrapper controlPlaneCmdWrapper) {
	ctrlInstance.replicationManager.processTryUpdateIsr()
	wrapper.completeCh <- struct{}{}
}
func (ce *controlPlaneExecutor) processShutdown(wrapper controlPlaneCmdWrapper) {
	// drain and discard events
	for {
		select {
		case e := <-ce.ctrlCmdPipe:
			ce.logger.Infof("Discarding event: %s", e.cmdtype.String())
		default:
			ce.isClosed = true
			close(ce.ctrlCmdPipe)
			ce.logger.Infof("channel cleared")
			wrapper.completeCh <- struct{}{}
			return
		}
	}
}

// func (ce *controlPlaneExecutor) processSetLeaderName(wrapper controlPlaneCmdWrapper) {
// 	leaderName := util.ParseZkLeader(wrapper.payload.(string)).Name
// 	ce.logger.Infof("Setting leader name = %s", leaderName)
// 	ctrlInstance.setCurrentLeaderName(leaderName)
// 	wrapper.completeCh <- struct{}{}
// }
// func (ce *controlPlaneExecutor) processSetNodeStatus(wrapper controlPlaneCmdWrapper) {
// 	param := wrapper.payload.(cmdPayload_SetNodeStatus)
// 	ce.logger.Infof("Setting node %s, isOffline = %t", param.nodeName, param.isOffline)
// 	if param.isOffline {
// 		ctrlInstance.addToOffline(param.nodeName)
// 	} else {
// 		ctrlInstance.removeFromOffline(param.nodeName)
// 	}
// 	wrapper.completeCh <- struct{}{}
// }

// func (ce *controlPlaneExecutor) processReceiveOffsetCollectRequest(wrapper controlPlaneCmdWrapper) {
// 	ctrlInstance.leaderElectionMgr.processReceiveOffsetCollectRequest()
// 	wrapper.completeCh <- struct{}{}
// }
// func (ce *controlPlaneExecutor) processReceiveRoleChangeRequest(wrapper controlPlaneCmdWrapper) {
// 	param := wrapper.payload.(cmdPayload_ReceiveRoleChangeRequest)
// 	ce.logger.Infof("Setting role to %s", param.role.String())
// 	rs := ctrlInstance.leaderElectionMgr.processReceiveRoleChangeRequest(param.role)
// 	*param.result = rs
// 	wrapper.completeCh <- struct{}{}
// }

func (ce *controlPlaneExecutor) processTestRequest(wrapper controlPlaneCmdWrapper) {
	fn := wrapper.payload.(func())
	fn()
	wrapper.completeCh <- struct{}{}
}

func (ce *controlPlaneExecutor) run() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(">>>> controlPlaneExecutor err: ", r)
		}
	}()
	for {
		cmdWrapper := <-ce.ctrlCmdPipe
		if cmdWrapper.cmdtype == ctrlCmd_Unknown {
			ce.logger.Infof("Channel has been closed")
			return
		}

		ce.logger.Debugf("Handling cmd type = %s ...", cmdWrapper.cmdtype.String())
		switch cmdWrapper.cmdtype {
		case ctrlCmd_NodeConnectionChanged:
			ce.processNodeConnectionChanged(cmdWrapper)
		case ctrlCmd_LeaderElection:
			ce.processLeaderElection(cmdWrapper)
		case ctrlCmd_AlterIsr:
			ce.processTryAlterIsr(cmdWrapper)
		// case ctrlCmd_SetLeaderName:
		// 	ce.processSetLeaderName(cmdWrapper)
		// case ctrlCmd_SetNodeStatus:
		// 	ce.processSetNodeStatus(cmdWrapper)
		// case ctrlCmd_ReceiveCollectOffsetRequest:
		// 	ce.processReceiveOffsetCollectRequest(cmdWrapper)
		// case ctrlCmd_ReceiveRoleChangeRequest:
		// 	ce.processReceiveRoleChangeRequest(cmdWrapper)
		case ctrlCmd_TestRequest:
			ce.processTestRequest(cmdWrapper)
		case ctrlCmd_Shutdown:
			ce.processShutdown(cmdWrapper)
		}
	}
}
