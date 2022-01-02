package simplekv

import (
	"time"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/util"
	"github.com/go-zookeeper/zk"
)

type controllerElectionManager struct {
	logger *util.Logger
}

func newControllerElectionManager() *controllerElectionManager {
	return &controllerElectionManager{
		logger: util.NewLogger("[ControllerElectionManager]", config.LogOutputWriter),
	}
}

// runOnce runs the election once again.
func (em *controllerElectionManager) runOnce() {
	em.logger.Infof("Controller election initiated")
	for {
		_, err := zkClient.Create(ZkPathControllerNode, []byte(ctrlInstance.nodeName), zk.FlagEphemeral, zkPermAll)
		if err != nil {
			if err == zk.ErrNodeExists {
				// Try get exist node name and save it to local storage.
				controllerName, err0 := zkGetControllerName()
				if err0 != nil {
					em.logger.Errorf("Get controller name from zk err: %s", err.Error())
					time.Sleep(config.RetryBackoff)
					continue
				}
				// Election failed, one of the other nodes has become leader, set controller name
				ctrlInstance.setCurrentControllerName(controllerName)
				if ctrlInstance.isController() {
					em.logger.Infof("Controller is myself: <%s>", ctrlInstance.nodeName) // This happens when a fast restart occur.
				} else {
					em.logger.Infof("Controller already exist, setting controller name to <%s>", controllerName)
				}
				break
			} else {
				// Net error, retry
				em.logger.Errorf("Create node err: %s", err)
				time.Sleep(config.RetryBackoff)
				continue
			}
		}
		break // Create success.
	}
	// Election success, mark myself as controller.
	em.logger.Infof("Controller election success, %s is controller", ctrlInstance.nodeName)
	ctrlInstance.setCurrentControllerName(ctrlInstance.nodeName)
}

// run a election campaign on a node to become controller.
func (em *controllerElectionManager) run() {
	var err error
	var exist bool
	var ch <-chan zk.Event
	// set watch, ignore error.
	// if success, try to consume from the event channel.
	// when consume success, enqueue and action to the control plane.
	var addWatchAndWait = func() {
		exist, _, ch, err = zkClient.ExistsW(ZkPathControllerNode)
		if err != nil {
			em.logger.Errorf("Get and set watch err: %s", err.Error())
			return
		}
		if !exist {
			em.logger.Errorf("ZkControllerNodePath = %s not exist", ZkPathControllerNode)
			return
		}
		ev := <-ch
		if ev.Err != nil {
			em.logger.Errorf("Get event from zkEvent channel err: %s", ev.Err.Error())
			return
		} else if ev.Type == zk.EventNodeDeleted {
			// on detect node lost, enqueue a controller clear action.
			em.logger.Errorf("Detect controller lost, path = %s", ev.Path)
			ctrlInstance.setCurrentControllerName("")
		}
	}

	for {
		em.logger.Infof("Controller election initiated")
		_, err = zkClient.Create(ZkPathControllerNode, []byte(ctrlInstance.nodeName), zk.FlagEphemeral, zkPermAll)
		if err != nil {
			if err == zk.ErrNodeExists {
				// Try get exist node name and save it to local storage.
				controllerName, err0 := zkGetControllerName()
				if err0 != nil {
					em.logger.Errorf("Get controller name from zk err: %s", err.Error())
					time.Sleep(config.RetryBackoff)
					continue
				}
				// Election failed, one of the other nodes has become leader, set controller name
				ctrlInstance.setCurrentControllerName(controllerName)
				if ctrlInstance.isController() {
					em.logger.Infof("Controller is myself: <%s>", ctrlInstance.nodeName) // This happens when a fast restart occur.
				} else {
					em.logger.Infof("Controller already exist, setting controller name to <%s>", controllerName)
				}
				addWatchAndWait()
			} else {
				em.logger.Errorf("Create node err: %s", err)
				time.Sleep(config.RetryBackoff) // net err, sleep and retry.
			}
		} else {
			// Election success, mark myself as controller.
			em.logger.Infof("Controller election success, %s is controller", ctrlInstance.nodeName)
			ctrlInstance.setCurrentControllerName(ctrlInstance.nodeName)
			addWatchAndWait() // election success, wait until the node gets deleted.
		}
	}
}
