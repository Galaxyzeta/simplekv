package simplekv

import (
	"context"
	"fmt"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/proto"
)

type SimplekvService struct{}

func getSuccessResponse() *proto.BaseResponse {
	return &proto.BaseResponse{
		Msg: "OK",
	}
}

func getSuccessResponseParam(param string) *proto.BaseResponse {
	return &proto.BaseResponse{
		Msg: param,
	}
}

func getErrorResponse(err string) *proto.BaseResponse {
	return &proto.BaseResponse{
		Code: 1,
		Msg:  err,
	}
}

func getRequiredAcks(ctx context.Context) int {
	v := ctx.Value("requiredAcks")
	if v == nil {
		return 0
	}
	ret, ok := v.(int)
	if !ok {
		return 0
	}
	return ret
}

func checkLeader() bool {
	return ctrlInstance != nil && ctrlInstance.isLeader()
}

func (*SimplekvService) Get(ctx context.Context, req *proto.GetRequest) (*proto.BaseResponse, error) {
	if !checkLeader() {
		return getErrorResponse(config.ErrNotLeader.Error()), nil
	}
	val, err := Get(req.Key)
	if err != nil {
		return getErrorResponse(err.Error()), nil
	}
	return getSuccessResponseParam(val), nil
}

func (*SimplekvService) Set(ctx context.Context, req *proto.SetRequest) (*proto.BaseResponse, error) {
	if !checkLeader() {
		return getErrorResponse(config.ErrNotLeader.Error()), nil
	}
	err := Write(req.Key, req.Value, getRequiredAcks(ctx))
	if err != nil {
		return getErrorResponse(err.Error()), nil
	}
	return getSuccessResponse(), nil
}

func (*SimplekvService) Del(ctx context.Context, req *proto.DelRequest) (*proto.BaseResponse, error) {
	if !checkLeader() {
		return getErrorResponse(config.ErrNotLeader.Error()), nil
	}
	err := Delete(req.Key, getRequiredAcks(ctx))
	if err != nil {
		return getErrorResponse(err.Error()), nil
	}
	return getSuccessResponse(), nil
}

func (*SimplekvService) Expire(ctx context.Context, req *proto.ExpireRequest) (*proto.BaseResponse, error) {
	if !checkLeader() {
		return getErrorResponse(config.ErrNotLeader.Error()), nil
	}
	err := Expire(req.Key, int(req.Ttl), getRequiredAcks(ctx))
	if err != nil {
		return getErrorResponse(err.Error()), nil
	}
	return getSuccessResponse(), nil
}

func (*SimplekvService) TTL(ctx context.Context, req *proto.TTLRequest) (*proto.BaseResponse, error) {
	if !checkLeader() {
		return getErrorResponse(config.ErrNotLeader.Error()), nil
	}
	ttl, err := TTL(req.Key)
	if err != nil {
		return getErrorResponse(err.Error()), nil
	}
	return getSuccessResponseParam(fmt.Sprintf("%d", ttl)), nil
}
