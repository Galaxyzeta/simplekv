package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/service/proto"
)

func handleBaseResponse(resp *proto.BaseResponse, err error) (string, error) {
	if err != nil {
		return "", err
	}
	if resp.Err != "" {
		return "", fmt.Errorf(resp.Err)
	}
	return resp.Msg, nil
}

func Get(ctx context.Context, param ...string) (string, error) {
	if len(param) != 1 {
		return "", config.ErrInvalidParam
	}
	resp, err := grpcClient.Get(ctx, &proto.GetRequest{
		Key: param[0],
	})
	return handleBaseResponse(resp, err)
}

func Set(ctx context.Context, param ...string) (string, error) {
	if len(param) != 2 {
		return "", config.ErrInvalidParam
	}
	resp, err := grpcClient.Set(ctx, &proto.SetRequest{
		Key:   param[0],
		Value: param[1],
	})
	return handleBaseResponse(resp, err)

}

func Del(ctx context.Context, param ...string) (string, error) {
	if len(param) != 1 {
		return "", config.ErrInvalidParam
	}
	resp, err := grpcClient.Del(ctx, &proto.DelRequest{
		Key: param[0],
	})
	return handleBaseResponse(resp, err)

}

func Expire(ctx context.Context, param ...string) (string, error) {
	if len(param) != 2 {
		return "", config.ErrInvalidParam
	}
	ttl, err := strconv.ParseInt(param[1], 10, 32)
	if err != nil {
		return "", err
	}
	resp, err := grpcClient.Expire(ctx, &proto.ExpireRequest{
		Key: param[0],
		Ttl: uint32(ttl),
	})
	return handleBaseResponse(resp, err)

}

func TTL(ctx context.Context, param ...string) (string, error) {
	if len(param) != 1 {
		return "", config.ErrInvalidParam
	}
	resp, err := grpcClient.TTL(ctx, &proto.TTLRequest{
		Key: param[0],
	})
	return handleBaseResponse(resp, err)

}
