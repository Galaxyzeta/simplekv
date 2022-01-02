package simplekv

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/proto"
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
	return os.Stdout.Write(np)
}

func testBootServerInProcess(cfgPath string) *exec.Cmd {
	return exec.Command("go", "run", "server/main.go", "-cfg", cfgPath)
}

func testBootServer() {
	testDeleteFolder()
	testBootServerNoDirDelete()
}

func testRestartDataPlane() {
	initDataPlaneSingleton()
	startDataPlaneSingleton()
}

func testBootServerNoDirDelete() {
	go Run(nil)
	time.Sleep(time.Second)
}

func testStopServer() {
	GracefulStop()
}

func testExecAsyncFuncTemplate(t *testing.T, fn func()) {
	testBootServer()
	wg := sync.WaitGroup{}
	wg.Add(1)
	ctrlInstance.cmdExecutor.enqueueAndWait_TestRequest(func() {
		fn()
		wg.Done()
	})
	wg.Wait()
	GracefulStop()
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
	cli := testGetDataClient(t, fmt.Sprintf("127.0.0.1:%d", config.NetDataPort))
	testBootServer()

	resp0, err := cli.Set(context.TODO(), &proto.SetRequest{
		Key:   "hello",
		Value: "1",
	}) // Write operation will cause watermark to rise.
	assert.NoError(t, err)
	assert.Equal(t, resp0.Code, int32(0))

	ctrlInstance.cmdExecutor.enqueueAndWait_TestRequest(func() {
		hw := dataInstance.varfp.ReadWatermarkFromCache()
		assert.True(t, hw != 0)
	})

	testStopServer()
}

func TestBootAndStopStandaloneServerUsingOsCmd(t *testing.T) {
	standaloneServer1 := testBootServerInProcess("conf/standalone/server-standalone.yaml")
	standaloneServer1.Stdout = os.Stdout
	go standaloneServer1.Run()
	time.Sleep(time.Second * 5)
	err := standaloneServer1.Process.Kill()
	if err != nil {
		t.Log(err)
	}
}

func TestBootClusterAndStopUsingCmd(t *testing.T) {
	node1 := testBootServerInProcess("conf/cluster2/server-node01.yaml")
	node2 := testBootServerInProcess("conf/cluster2/server-node02.yaml")
	node1.Stdout = testLabeledOsStdout{
		name: "Node1",
	}
	node2.Stdout = testLabeledOsStdout{
		name: "Node2",
	}
	// node2.Stdout = os.Stdout
	var err1, err2 error
	go func() { err1 = node1.Run() }() // node1 will become leader
	time.Sleep(time.Second)
	go func() { err2 = node2.Run() }()
	time.Sleep(time.Second * 5)
	if err1 != nil {
		panic(err1)
	}
	if err2 != nil {
		panic(err2)
	}
	err := node1.Process.Kill()
	if err != nil {
		panic(err)
	}
	err = node2.Process.Kill()
	if err != nil {
		panic(err)
	}
}
