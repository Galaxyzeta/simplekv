package simplekv

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/proto"
	"github.com/galaxyzeta/simplekv/util"
	"google.golang.org/grpc"
)

var _dataplaneServer *grpc.Server
var _controlPlaneServer *grpc.Server

var serverStopNotifier chan struct{} = make(chan struct{}, 1)

// Run is the entry point of a SimpleKV server.
func Run(afterLoadingConfig func(), cfgPath string, needParseFlag bool) {

	var mainLogger = util.NewLogger("[Main]", config.LogOutputWriter)

	if needParseFlag {
		if cfgPath == "" {
			cfgPath = "conf/standalone/server-standalone.yaml"
		}
		flag.String("cfg", cfgPath, "configuration file")
		flag.Parse()
	}

	config.InitCfg("")
	if afterLoadingConfig != nil {
		afterLoadingConfig() // to override configs for testings purpose.
	}

	var dataPlaneServerIpport = fmt.Sprintf("localhost:%d", config.NetDataPort)
	var controlPlaneServerIpport = fmt.Sprintf("localhost:%d", config.NetControlPort)
	var wg = sync.WaitGroup{}
	var signalTerminationChannel = make(chan os.Signal, 2)

	// This is used to block data serving to users util all stuff has been set up.
	var condStartServingData = util.NewConditionBlocker(func() bool { return false })

	// boot up data server.
	var runDataplaneServer = func() {
		defer wg.Done()
		condStartServingData.WaitOnceIfTrue() // block until everything's ready

		lis, err := net.Listen("tcp", dataPlaneServerIpport)
		if err != nil {
			mainLogger.Errorf("fatal while booting dataPlane server: %s", err.Error())
			os.Exit(1)
		}
		mainLogger.Infof("Dataplane server has started listening on %s", dataPlaneServerIpport)

		_dataplaneServer = grpc.NewServer()
		proto.RegisterSimpleKVServer(_dataplaneServer, &SimplekvService{})
		err = _dataplaneServer.Serve(lis)
		if err != nil {
			mainLogger.Errorf("fatal while booting dataPlane server: %s", err.Error())
			os.Exit(1)
		}
	}

	// boot up control signal server.
	var runControlPlaneServer = func() {
		defer wg.Done()

		lis, err := net.Listen("tcp", controlPlaneServerIpport)
		if err != nil {
			mainLogger.Errorf("fatal while booting controlPlane server: %s", err.Error())
			os.Exit(1)
		}
		mainLogger.Infof("ControlPlane server has started listening on %s", controlPlaneServerIpport)

		_controlPlaneServer = grpc.NewServer()
		proto.RegisterControlPlaneServiceServer(_controlPlaneServer, &ControlPlaneService{})
		err = _controlPlaneServer.Serve(lis)
		if err != nil {
			mainLogger.Errorf("fatal while booting controlPlane server: %s", err.Error())
			os.Exit(1)
		}
	}

	// monitor user termination signal to gracefully stop the servers.
	// This goroutine will not be waited when shutting down server.
	var sigkillMonitor = func() {
		signal.Notify(chan<- os.Signal(signalTerminationChannel), syscall.SIGINT, syscall.SIGTERM)
		for sig := range signalTerminationChannel {
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM:
				mainLogger.Infof("Termination signal captured: Signal<%v>. Starting to terminate all goroutines.", sig)
				ShutdownServerGracefully(false)
				mainLogger.Infof("All services has been terminated, exiting...")
				return // terminated, trigger wg.Done and breakout.
			}
		}
	}

	// always block until control server is up.
	var waitUntilControlServerStart = func() {
		conn, err := grpc.Dial(controlPlaneServerIpport, grpc.WithBlock(), grpc.WithInsecure())
		if err != nil {
			panic(err)
		}
		if err = conn.Close(); err != nil {
			mainLogger.Errorf("Close test connection err: %s", err.Error())
		}
	}

	wg.Add(2) // Wait until dataPlaneServer and controlPlaneServer both got shutted down.
	zkMustInit()
	initControlPlaneSingleton()
	initDataPlaneSingleton()
	go runDataplaneServer()
	go runControlPlaneServer()
	go sigkillMonitor()
	waitUntilControlServerStart()
	startControlPlaneSingleton() // Must start server first
	startDataPlaneSingleton()
	condStartServingData.Broadcast() // start serving data because everything was ready.
	wg.Wait()

	// system exited normally.
	serverStopNotifier <- struct{}{}
	mainLogger.Infof("System exited OK !")
}

// Shutdown the server gracefully. Will not wait until the server really stops.
func ShutdownServerGracefully(blockUntilClose bool) {
	fmt.Println(">>>> System shutdown command got detected !")
	_dataplaneServer.GracefulStop()
	_controlPlaneServer.GracefulStop()
	dataInstance.shutdownGracefully()
	ctrlInstance.ShutdownGracefully()
	if blockUntilClose {
		<-serverStopNotifier
	}
	zkMustShutdown()
	fmt.Println(">>>> System exited OK !")
}
