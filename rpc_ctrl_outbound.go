package simplekv

import (
	"context"
	"fmt"
	"sync"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/proto"
	"google.golang.org/grpc"
)

type controlPlaneRpcManager struct {
	controlPlaneClients sync.Map
}

func newControlPlaneRpcManager(hostports []string) *controlPlaneRpcManager {
	rpcManager := controlPlaneRpcManager{}
	for _, hostport := range hostports {
		conn, err := grpc.Dial(hostport, grpc.WithInsecure()) // TODO safety
		if err != nil {
			panic(err)
		}
		rpcManager.controlPlaneClients.Store(hostport, proto.NewControlPlaneServiceClient(conn))
	}
	return &rpcManager
}

func (c *controlPlaneRpcManager) client(hostport string) (proto.ControlPlaneServiceClient, error) {
	client, ok := c.controlPlaneClients.Load(hostport)
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
	}
	return resp.Data, resp.LeaderHw, nil
}

func (c *controlPlaneRpcManager) roleChange(ctx context.Context, hostport string, role proto.ServerRole) error {
	cli, err := c.client(hostport)
	if err != nil {
		return err
	}
	resp, err := cli.RoleChange(ctx, &proto.RoleChangeRequest{
		Role: role,
	})
	if err != nil {
		return err
	} else if err = c._handleBaseResp(resp.BaseResp); err != nil {
		return err
	}
	return nil
}

func (c *controlPlaneRpcManager) collectOffset(ctx context.Context, hostport string) (int64, error) {
	cli, err := c.client(hostport)
	if err != nil {
		return 0, err
	}
	resp, err := cli.CollectOffset(ctx, &proto.CollectOffsetRequest{})
	if err != nil {
		return 0, err
	} else if err = c._handleBaseResp(resp.BaseResp); err != nil {
		return 0, err
	}
	return resp.Offset, nil
}

func (c *controlPlaneRpcManager) collectWatermark(ctx context.Context, hostport string) (int64, error) {
	cli, err := c.client(hostport)
	if err != nil {
		return 0, err
	}
	resp, err := cli.CollectWatermark(ctx, &proto.CollectWatermarkRequest{})
	if err != nil {
		return 0, err
	} else if err = c._handleBaseResp(resp.BaseResp); err != nil {
		return 0, err
	}
	return resp.GetHwm(), nil
}
