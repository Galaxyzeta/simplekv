package simplekv

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/proto"
	"github.com/galaxyzeta/simplekv/util"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

type testLabeledOsStdout struct {
	name string
}

func (lb testLabeledOsStdout) Write(p []byte) (n int, err error) {
	prefixLen := len(lb.name) + 6
	np := make([]byte, prefixLen+len(p))
	copy(np[:prefixLen], []byte(fmt.Sprintf("[%s] >> ", lb.name)))
	copy(np[prefixLen:], p)
	n, _ = os.Stdout.Write(np)
	return n, nil
}

func testBootServerInProcess(cfgPath string) *exec.Cmd {
	return exec.Command("go", "run", "server/main.go", "-cfg", cfgPath)
}

func testRestartServerAndWaitUntilOK() {
	// server restart and wait until OK
	ShutdownServerGracefully(true)
	testInternalBootServer("conf/standalone/server-standalone.yaml", false, true, false)
}

func testBootServerWithCfgPath(cfg string) {
	testInternalBootServer(cfg, true, true, true)
}

func testBootServerStandalone(additionalFunction ...func()) {
	testInternalBootServer("conf/standalone/server-standalone.yaml", true, true, true)
}

func testBootDataClient(hostport string) proto.SimpleKVClient {
	conn, err := grpc.Dial(hostport, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	return proto.NewSimpleKVClient(conn)
}

// Boot up standalone server.
// Params:
// - cleanBoot: delete all zk path and data folder.
// - shouldWait: wait until dataInstance.state == Ready
// - parseFlag: should we parse the flag. When there's a manual restart in testing, this should be set to false
// 	 						avoid flag redefined problem.
func testInternalBootServer(cfg string, cleanBoot bool, shouldWait bool, parseFlag bool) {
	if cleanBoot {
		zkMustInit()
		zkDeleteRootDir()
		// Remove data dir
		err := os.RemoveAll(config.DataDir)
		if err != nil {
			panic(err)
		}
	}
	go Run(nil, cfg, parseFlag)
	for ctrlInstance == nil {
		time.Sleep(time.Millisecond * 250)
	}
	if shouldWait {
		for ctrlInstance == nil {
			time.Sleep(time.Millisecond * 50)
		}
		for dataInstance == nil {
			time.Sleep(time.Millisecond * 50)
		}
		ctrlInstance.condHasLeader.WaitOnceIfFalse()
		for !dataInstance.isReady() {
			time.Sleep(time.Millisecond * 50)
		}
	}
}

func testKillProcessWindows(pid int) error {
	kill := exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(pid))
	return kill.Run()
}

func testExecAsyncFuncTemplate(t *testing.T, fn func()) {
	// testBootServer()
	// wg := sync.WaitGroup{}
	// wg.Add(1)
	// ctrlInstance.cmdExecutor.enqueueAndWait_TestRequest(func() {
	// 	fn()
	// 	wg.Done()
	// })
	// wg.Wait()
	// GracefulStop()
}

func testGetDataClient(t *testing.T, hostport string) proto.SimpleKVClient {
	conn, err := grpc.Dial(hostport, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	client := proto.NewSimpleKVClient(conn)
	return client
}

func TestLogFetchDelayer(t *testing.T) {
	tm := time.Now()
	ld := newLogFetchDelayer(time.Second)
	ld.notifyAndDequeue(100)
	entry5 := ld.enqueue(5)
	entry6 := ld.enqueue(6)
	entry4 := ld.enqueue(4)
	entry1 := ld.enqueue(1)
	entry3 := ld.enqueue(3)
	ld.notifyAndDequeue(1)
	assert.True(t, entry1.wait())
	ld.notifyAndDequeue(2)
	ld.notifyAndDequeue(3)
	assert.True(t, entry3.wait())
	ld.notifyAndDequeue(5)
	assert.True(t, entry4.wait())
	assert.True(t, entry5.wait())
	ld.notifyAndDequeue(1000)
	assert.True(t, entry6.wait())
	cost := time.Since(tm)
	assert.True(t, cost < time.Millisecond*5) // should not trigger any wait.
}

func TestVarFileIO(t *testing.T) {
	// cli := testGetDataClient(t, fmt.Sprintf("127.0.0.1:%d", config.NetDataPort))
	// testBootServer()

	// resp0, err := cli.Set(context.TODO(), &proto.SetRequest{
	// 	Key:   "hello",
	// 	Value: "1",
	// }) // Write operation will cause watermark to rise.
	// assert.NoError(t, err)
	// assert.Equal(t, resp0.Code, int32(0))

	// ctrlInstance.cmdExecutor.enqueueAndWait_TestRequest(func() {
	// 	hw := dataInstance.varfp.ReadWatermarkFromCache()
	// 	assert.True(t, hw != 0)
	// })

	// testStopServer()
}

func TestBootAndStopStandaloneServerUsingOsCmd(t *testing.T) {

	standaloneServer1 := testBootServerInProcess("conf/standalone/server-standalone.yaml")
	standaloneServer1.Stdout = os.Stdout
	standaloneServer1.Stderr = os.Stdout
	err := standaloneServer1.Start()
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	time.Sleep(time.Second * 5)
	t.Log(standaloneServer1.Process.Pid)
	err = testKillProcessWindows(standaloneServer1.Process.Pid)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
}

func TestStats(t *testing.T) {
	statsManager := newStatsManager()
	go statsManager.run()
	statsManager.recordLogCommit(time.Second)
	statsManager.recordLogCommit(time.Second)
	statsManager.recordLogCommit(time.Second)
	statsManager.recordLogCommit(time.Second)
	statsManager.recordLogCommit(time.Second)
	time.Sleep(time.Second)
	t.Log(statsManager.getCommitStats().toString())
}

func TestLogReplication(t *testing.T) {

	zkMustInit()
	if e := zkDeleteRecursively("/simplekv"); e != nil {
		t.Logf(e.Error())
		t.FailNow()
	}
	zkMustShutdown()

	testBootServerWithCfgPath("conf/cluster2/server-node01.yaml")
	node2 := testBootServerInProcess("conf/cluster2/server-node02.yaml")

	fp2, _ := os.OpenFile("tmp/test2.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	node2.Stdout = fp2
	node2.Stderr = fp2

	// node2.Stdout = os.Stdout
	time.Sleep(time.Millisecond * 500)
	node2.Start()
	defer testKillProcessWindows(node2.Process.Pid)
	time.Sleep(time.Second * 5)

	hasErr := false
	wg := sync.WaitGroup{}

	const itercount = 100
	const requireIsr = 1

	for i := range util.Iter(itercount) {
		wg.Add(1)
		key := fmt.Sprintf("log%d", i)
		value := fmt.Sprintf("value%d", i)
		go func() {
			err := Write(key, value, requireIsr)
			if err != nil {
				t.Log(err)
				hasErr = true
			}
			wg.Done()
		}()
	}
	if hasErr {
		t.FailNow()
	}
	wg.Wait()
	for i := range util.Iter(itercount) {
		key := fmt.Sprintf("log%d", i)
		v, err := Get(key)
		if err != nil {
			t.Logf("key = %s failed: %s", key, err)
			t.FailNow()
		}
		if v != fmt.Sprintf("value%d", i) {
			t.Log("not equal")
			t.FailNow()
		}
	}

	time.Sleep(time.Second * 2)
	commitStatsHolder := ctrlInstance.stats.getCommitStats()
	t.Log(commitStatsHolder.toString())

	ShutdownServerGracefully(true) // Kill server 1, test server 2 data.
	cli := testBootDataClient("127.0.0.1:7779")
	time.Sleep(time.Second * 8) // Wait for leader election

	mustGetFromServer2 := func(key string) (string, error) {
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		resp, err := cli.Get(ctx, &proto.GetRequest{
			Key: key,
		})
		if err != nil {
			return "", err
		}
		if resp.Code != 0 {
			return "", fmt.Errorf(resp.Msg)
		}
		return resp.Msg, err
	}

	for i := range util.Iter(itercount) {
		key := fmt.Sprintf("log%d", i)
		v, err := mustGetFromServer2(key)
		if err != nil {
			t.Logf("key = %s failed: %s", key, err)
			t.FailNow()
		}
		if v != fmt.Sprintf("value%d", i) {
			t.Log("not equal")
			t.FailNow()
		}
	}

	t.Log("data verification OK")

}

func TestBootServerAndShutdown(t *testing.T) {
	testBootServerStandalone()
	ShutdownServerGracefully(true)
	// Attempt to see "System exited OK" in the console.
}

// Restart the server, will not delete all ZK path and datadir.
func TestBootServerAndRestart(t *testing.T) {
	testBootServerStandalone(nil)
	testRestartServerAndWaitUntilOK()
	ShutdownServerGracefully(true)
}

func TestLeaderElection(t *testing.T) {
	testBootServerWithCfgPath("conf/cluster2/server-node01.yaml")
	node2 := testBootServerInProcess("conf/cluster2/server-node02.yaml")
	fp2, _ := os.OpenFile("tmp/test2.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	node2.Stdout = fp2
	node2.Stderr = fp2
	node2.Start()
	defer testKillProcessWindows(node2.Process.Pid)

	time.Sleep(time.Second * 5)
	isrList := ctrlInstance.replicationManager.cloneIsrList(true)
	// Warning: magic string
	assert.Equal(t, isrList[0], "node01")
	assert.Equal(t, isrList[1], "node02")

	ShutdownServerGracefully(true)
}

func TestBootClusterAndStopUsingCmd(t *testing.T) {
	// Delete ISR node.
	zkMustInit()
	zkDeleteIsr()

	node1 := testBootServerInProcess("conf/cluster2/server-node01.yaml")
	node2 := testBootServerInProcess("conf/cluster2/server-node02.yaml")

	fp1, _ := os.OpenFile("tmp/test1.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	fp2, _ := os.OpenFile("tmp/test2.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	node1.Stdout = fp1
	node1.Stderr = fp1
	node2.Stdout = fp2
	node2.Stderr = fp2

	// node2.Stdout = os.Stdout
	node1.Start()
	time.Sleep(time.Second)
	node2.Start()
	time.Sleep(time.Second * 5)
	testKillProcessWindows(node1.Process.Pid)
	testKillProcessWindows(node2.Process.Pid)
}
