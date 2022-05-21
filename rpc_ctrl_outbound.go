package simplekv

import (
	"context"
	"fmt"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/dbfile"
	"github.com/galaxyzeta/simplekv/proto"
	"google.golang.org/grpc"
)

type controlPlaneRpcManager struct {
	controlPlaneClients map[string]proto.ControlPlaneServiceClient // readonly after init
}

func newControlPlaneRpcManager(hostports []string) *controlPlaneRpcManager {
	rpcManager := controlPlaneRpcManager{}
	rpcManager.controlPlaneClients = make(map[string]proto.ControlPlaneServiceClient)
	for _, hostport := range hostports {
		conn, err := grpc.Dial(hostport, grpc.WithInsecure()) // TODO safety
		if err != nil {
			panic(err)
		}
		rpcManager.controlPlaneClients[hostport] = proto.NewControlPlaneServiceClient(conn)
	}
	return &rpcManager
}

func (c *controlPlaneRpcManager) client(hostport string) (proto.ControlPlaneServiceClient, error) {
	client, ok := c.controlPlaneClients[hostport]
	if !ok {
		return nil, config.ErrRecordNotFound
	}
	return client.(proto.ControlPlaneServiceClient), nil
}

func (c *controlPlaneRpcManager) _handleBaseResp(resp *proto.BaseResponse) error {
	if resp == nil {
		return fmt.Errorf("empty baseResp")
	}
	if resp.Code != 0 {
		return fmt.Errorf(resp.Msg)
	}
	return nil
}

func (c *controlPlaneRpcManager) fetchLog(ctx context.Context, hostport string, offsetFrom int64, count int64) ([][]byte, int64, error) {
	cli, err := c.client(hostport)
	if err != nil {
		return nil, 0, err
	}
	resp, err := cli.FetchLog(ctx, &proto.FetchLogRequest{
		CallerNodeName: ctrlInstance.nodeName,
		OffsetFrom:     offsetFrom,
		Count:          count,
	})
	if err != nil {
		return nil, 0, err
	} else if err = c._handleBaseResp(resp.BaseResp); err != nil {
		return nil, 0, err
	} else if ctrlInstance.isLeaderEpochStaleAndTryUpdate(int(resp.LeaderEpoch)) {
		return nil, 0, config.ErrStaleLeaderEpoch
	}
	return resp.Data, resp.LeaderHw, nil
}

func (c *controlPlaneRpcManager) collectLeaderEpochOffset(ctx context.Context, hostport string, myLeaderEpoch int64) (dbfile.LeaderEpochAndOffset, error) {
	cli, err := c.client(hostport)
	if err != nil {
		return dbfile.LeaderEpochAndOffset{}, err
	}
	resp, err := cli.CollectLeaderEpochAndOffset(ctx, &proto.CollectLeaderEpochAndOffsetRequest{
		MyLeaderEpoch: myLeaderEpoch,
	})
	if err != nil {
		return dbfile.LeaderEpochAndOffset{}, err
	} else if err = c._handleBaseResp(resp.BaseResp); err != nil {
		return dbfile.LeaderEpochAndOffset{}, err
	} else if ctrlInstance.isLeaderEpochStaleAndTryUpdate(int(resp.LeaderEpoch)) {
		return dbfile.LeaderEpochAndOffset{}, config.ErrStaleLeaderEpoch
	}
	return dbfile.LeaderEpochAndOffset{
		LeaderEpoch: resp.LeaderEpoch,
		Offset:      resp.Offset,
	}, nil
}
