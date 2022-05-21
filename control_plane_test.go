package simplekv

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/galaxyzeta/simplekv/client"
	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/proto"
	"github.com/galaxyzeta/simplekv/util"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

type testLabeledOsStdout struct {
	name string
}

func testSendCtrlBreak(pid int) error {
	d, e := syscall.LoadDLL("kernel32.dll")
	if e != nil {
		return e
	}
	p, e := d.FindProc("GenerateConsoleCtrlEvent")
	if e != nil {
		return e
	}
	r, _, e := p.Call(syscall.CTRL_BREAK_EVENT, uintptr(pid))
	if r == 0 {
		return fmt.Errorf("GenerateConsoleCtrlEvent: %v\n", e.Error())
	}
	return nil
}

func testMustRemoveZkNodes() {
	zkMustInit()
	if e := zkDeleteRecursively("/simplekv"); e != nil {
		panic(e)
	}
	zkMustShutdown()
}

// This will delete all contents under tmp folder
func testDeleteTmpDir() {
	os.RemoveAll("tmp")
	os.Mkdir("tmp", os.ModePerm)
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

// This is not a clean boot !
func testRestartServerAndWaitUntilOK() {
	// server restart and wait until OK
	ShutdownServerGracefully(true)
	testInternalBootServer("conf/standalone/server-standalone.yaml", false, true, false, nil)
}

func testBootServerWithCfgPath(cfg string) {
	testInternalBootServer(cfg, true, true, true, nil)
}

// Boot a standalone server cleanly. (Delete zk node and related data files)
func testBootServerStandalone(additionalFunction ...func()) {
	var realFunc func()
	if len(additionalFunction) != 0 {
		realFunc = additionalFunction[0]
	}
	testInternalBootServer("conf/standalone/server-standalone.yaml", true, true, true, realFunc)
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
// to avoid flag redefined problem.
func testInternalBootServer(cfg string, cleanBoot bool, shouldWait bool, parseFlag bool, afterLoadingCfg func()) {
	if cleanBoot {
		zkMustInit()
		zkDeleteRootDir()
		// Remove data dir
		err := os.RemoveAll(config.DataDir)
		if err != nil {
			panic(err)
		}
	}
	go Run(afterLoadingCfg, cfg, parseFlag)
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
	if err := testSendCtrlBreak(pid); err != nil {
		fmt.Printf("sigterm failed: %s, try force kill\n", err.Error())
	} else {
		return nil
	}
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

func Test3NodeLogReplicationWithMySelfControllable(t *testing.T) {
	testMustRemoveZkNodes()
	testDeleteTmpDir()

	testBootServerWithCfgPath("conf/cluster3/server-node01.yaml")

	node2 := testBootServerInProcess("conf/cluster3/server-node02.yaml")
	fp2, _ := os.OpenFile("tmp/test2.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	node2.Stdout = fp2
	node2.Stderr = fp2
	node2.Start()
	defer testKillProcessWindows(node2.Process.Pid)

	node3 := testBootServerInProcess("conf/cluster3/server-node03.yaml")
	fp3, _ := os.OpenFile("tmp/test3.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	node3.Stdout = fp3
	node3.Stderr = fp3
	node3.Start()
	defer testKillProcessWindows(node3.Process.Pid)

	time.Sleep(time.Second * 5)
	const iteration = 1000

	testTimeEstimate(t, func() {
		wg := sync.WaitGroup{}
		for i := range util.Iter(iteration) {
			wg.Add(1)
			go func(logidx int) {
				Write(fmt.Sprintf("hello-%d", logidx), "world", -1)
				wg.Done()
			}(i)
		}
		wg.Wait()
	})

	ShutdownServerGracefully(true)
}

func Test3NodeLogReplication(t *testing.T) {
	testMustRemoveZkNodes()
	testDeleteTmpDir()
	fps := make([]*os.File, 3)
	nodes := make([]*exec.Cmd, 3)
	for i := range util.Iter(3) {
		fps[i], _ = os.OpenFile(fmt.Sprintf("tmp/test%d.log", i), os.O_CREATE|os.O_WRONLY, os.ModePerm)
		nodes[i] = testBootServerInProcess(fmt.Sprintf("conf/cluster3/server-node0%d.yaml", i+1))
		nodes[i].Stderr = fps[i]
		nodes[i].Stdout = fps[i]
		e := nodes[i].Start()
		defer testKillProcessWindows(nodes[i].Process.Pid)
		if e != nil {
			t.Logf("%s", e.Error())
			t.Fail()
		}
		time.Sleep(time.Second)
	}
	client.Startup("conf/client/cli.yaml")
	var err error
	var cli proto.SimpleKVClient
	for true {
		cli, err = client.Client()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	time.Sleep(time.Second * 5)

	const iterCount = 5
	const requiredAck = -1
	ctx := context.Background()

	clientSetNoErr := func(k, v string) {
		timeEst := time.Now()
		_, err0 := cli.Set(ctx, &proto.SetRequest{
			Key:          k,
			Value:        v,
			RequiredAcks: -1,
		})
		if err != nil {
			t.Fail()
			t.Log(err0)
		}
		fmt.Printf("Set kv = %s, %s takes %d ms\n", k, v, time.Since(timeEst).Milliseconds())
	}

	clientGetNoErrAndExpect := func(k, v string) {
		rv, err0 := cli.Get(ctx, &proto.GetRequest{
			Key: k,
		})
		if err != nil {
			t.Fail()
			t.Log(err0)
			return
		}
		if rv == nil {
			t.Fail()
			t.Log("response is nil")
		}
		if rv.Code != 0 {
			t.Fail()
			t.Log(rv.Msg)
			return
		}
		if rv.Msg != v {
			t.Fail()
			t.Logf("value not match! expect to get <%s, %s>, actual is <%s, %s>", k, v, k, rv.Msg)
			return
		}
	}

	wg := sync.WaitGroup{}
	for i := range util.Iter(iterCount) {
		key := fmt.Sprintf("log%d", i)
		value := fmt.Sprintf("value%d", i)
		wg.Add(1)
		go func() {
			clientSetNoErr(key, value) // block wait
			wg.Done()
		}()
	}
	wg.Wait()

	for i := range util.Iter(iterCount) {
		key := fmt.Sprintf("log%d", i)
		value := fmt.Sprintf("value%d", i)
		wg.Add(1)
		go func() {
			clientGetNoErrAndExpect(key, value)
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestLogReplication(t *testing.T) {

	testMustRemoveZkNodes()
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

	const itercount = 10
	const requireAck = -1

	for i := range util.Iter(itercount) {
		wg.Add(1)
		key := fmt.Sprintf("log%d", i)
		value := fmt.Sprintf("value%d", i)
		go func() {
			err := Write(key, value, requireAck)
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

func TestTerminateProcess(t *testing.T) {
	testMustRemoveZkNodes()
	cmd := testBootServerInProcess("conf/standalone/server-standalone.yaml")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Start()
	time.Sleep(time.Second * 5)
	err := testKillProcessWindows(cmd.Process.Pid)
	if err != nil {
		t.Log(err)
	}
	time.Sleep(time.Second * 5)
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
	time.Sleep(time.Second * 5)

	zkMustInit()
	epoch, err := zkGetLeaderEpoch()
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	assert.Equal(t, 2, epoch)
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

func TestISRLost(t *testing.T) {
	testMustRemoveZkNodes()
	node1 := testBootServerInProcess("conf/cluster2/server-node01.yaml")
	node2 := testBootServerInProcess("conf/cluster2/server-node02.yaml")
	fp1, _ := os.OpenFile("tmp/test1.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	fp2, _ := os.OpenFile("tmp/test2.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	fp1.Truncate(0)
	fp2.Truncate(0)
	node1.Stdout = fp1
	node1.Stderr = fp1
	node2.Stdout = fp2
	node2.Stderr = fp2
	node1.Start()
	time.Sleep(time.Second)
	node2.Start()

	t.Logf("process1's pid = %d", node1.Process.Pid)
	t.Logf("process2's pid = %d", node2.Process.Pid)

	cli := testBootDataClient("127.0.0.1:6669")

	for {
		resp, err := cli.Set(context.Background(), &proto.SetRequest{
			Key:   "hello",
			Value: "1",
		})
		if err != nil {
			time.Sleep(time.Microsecond * 100)
			continue
		}
		if resp.Code != 0 {
			time.Sleep(time.Microsecond * 100)
			continue
		}
		break
	}
	t.Log("Log write OK")

	time.Sleep(time.Second * 5)
	if err := testKillProcessWindows(node2.Process.Pid); err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 5)
	if err := testKillProcessWindows(node1.Process.Pid); err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 5)
	t.Log("checkpoint")
}
