package main

import (
	"context"
	"fmt"

	"github.com/galaxyzeta/simplekv"
	"github.com/galaxyzeta/simplekv/service/proto"
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
		Err: err,
	}
}

func (*SimplekvService) Get(ctx context.Context, req *proto.GetRequest) (*proto.BaseResponse, error) {
	val, err := simplekv.Get(req.Key)
	if err != nil {
		return getErrorResponse(err.Error()), nil
	}
	return getSuccessResponseParam(val), nil
}

func (*SimplekvService) Set(ctx context.Context, req *proto.SetRequest) (*proto.BaseResponse, error) {
	err := simplekv.Write(req.Key, req.Value)
	if err != nil {
		return getErrorResponse(err.Error()), nil
	}
	return getSuccessResponse(), nil
}

func (*SimplekvService) Del(ctx context.Context, req *proto.DelRequest) (*proto.BaseResponse, error) {
	err := simplekv.Delete(req.Key)
	if err != nil {
		return getErrorResponse(err.Error()), nil
	}
	return getSuccessResponse(), nil
}

func (*SimplekvService) Expire(ctx context.Context, req *proto.ExpireRequest) (*proto.BaseResponse, error) {
	err := simplekv.Expire(req.Key, int(req.Ttl))
	if err != nil {
		return getErrorResponse(err.Error()), nil
	}
	return getSuccessResponse(), nil
}

func (*SimplekvService) TTL(ctx context.Context, req *proto.TTLRequest) (*proto.BaseResponse, error) {
	ttl, err := simplekv.TTL(req.Key)
	if err != nil {
		return getErrorResponse(err.Error()), nil
	}
	return getSuccessResponseParam(fmt.Sprintf("%d", ttl)), nil
}