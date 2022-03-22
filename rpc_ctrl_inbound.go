package simplekv

import (
	"context"

	"github.com/galaxyzeta/simplekv/proto"
)

// ControlPlaneService is an implementation of control plane services.
// It answers rpc calls about distribute problems.
type ControlPlaneService struct{}

func (*ControlPlaneService) FetchLog(ctx context.Context, req *proto.FetchLogRequest) (*proto.FetchLogResponse, error) {
	data, err := ctrlInstance.replicationManager.onReceiveLogFetchRequest(req.CallerNodeName, req.OffsetFrom, req.Count)
	resp := &proto.FetchLogResponse{
		Data:     data,
		LeaderHw: dataInstance.vars.ReadWatermarkFromCache(),
		BaseResp: &proto.BaseResponse{},
	}
	if err != nil {
		resp.BaseResp.Code = 1 // TODO do something with error code
		resp.BaseResp.Msg = err.Error()
	}
	return resp, nil
}

func (*ControlPlaneService) RoleChange(ctx context.Context, req *proto.RoleChangeRequest) (*proto.RoleChangeResponse, error) {
	// ctrlInstance.leaderElectionMgr.onReceiveRoleChangeRequest(req.Role)
	return &proto.RoleChangeResponse{
		BaseResp: &proto.BaseResponse{},
	}, nil
}

func (*ControlPlaneService) CollectOffset(ctx context.Context, req *proto.CollectOffsetRequest) (*proto.CollectOffsetResponse, error) {
	// ctrlInstance.leaderElectionMgr.onReceiveOffsetCollectRequest()
	return &proto.CollectOffsetResponse{
		Offset:   dataInstance.totalOffset(),
		BaseResp: &proto.BaseResponse{},
	}, nil
}

func (*ControlPlaneService) CollectWatermark(ctx context.Context, req *proto.CollectWatermarkRequest) (*proto.CollectWatermarkResponse, error) {
	hwm, err := ctrlInstance.replicationManager.onReceiveCollectWatermarkRequest()
	resp := &proto.CollectWatermarkResponse{
		Hwm:      hwm,
		BaseResp: &proto.BaseResponse{},
	}
	if err != nil {
		resp.BaseResp.Code = 1
		resp.BaseResp.Msg = err.Error()
	}
	return resp, nil
}

func (*ControlPlaneService) CollectLeaderEpochAndOffset(ctx context.Context, req *proto.CollectLeaderEpochAndOffsetRequest) (*proto.CollectLeaderEpochAndOffsetResponse, error) {
	data, err := ctrlInstance.leaderElectionMgr.onReceiveGetLeaderEpochAndLogOffsetRequest(req.MyLeaderEpoch)
	resp := &proto.CollectLeaderEpochAndOffsetResponse{
		LeaderEpoch: data.LeaderEpoch,
		Offset:      data.Offset,
		BaseResp:    &proto.BaseResponse{},
	}
	if err != nil {
		resp.BaseResp.Code = 1
		resp.BaseResp.Msg = err.Error()
	}
	return resp, nil
}
