package client

import (
	"context"
	"fmt"
	"strconv"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/proto"
	"google.golang.org/grpc"
)

var connections map[string]proto.SimpleKVClient = make(map[string]proto.SimpleKVClient)

// Client returns the Client that corresponding to current leader hostport.
// If doesn't exist, will create one and add it to the cache.
func Client() (proto.SimpleKVClient, error) {
	hostport := currentLeaderHostport()
	if hostport == "" {
		return nil, config.ErrNoLeaderFound
	} else {
		// Fetch connections from conn pool.
		cli, ok := connections[hostport]
		if !ok {
			// Register a new connection to the cache.
			// Modify port to be the one which handles data
			conn, err := grpc.Dial(hostport, grpc.WithInsecure())
			if err != nil {
				return nil, err
			}
			cli = proto.NewSimpleKVClient(conn)
			connections[hostport] = cli
		}
		return cli, nil
	}
}

func handleBaseResponse(resp *proto.BaseResponse, err error) (string, error) {
	if err != nil {
		return "", err
	}
	if resp.Code != 0 {
		return "", fmt.Errorf(resp.Msg)
	}
	return resp.Msg, nil
}

func Get(ctx context.Context, param ...string) (string, error) {
	if len(param) != 1 {
		return "", config.ErrInvalidParam
	}

	grpcClient, err := Client()
	if err != nil {
		return "", err
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

	grpcClient, err := Client()
	if err != nil {
		return "", err
	}
	resp, err := grpcClient.Set(ctx, &proto.SetRequest{
		Key:          param[0],
		Value:        param[1],
		RequiredAcks: ack,
	})
	return handleBaseResponse(resp, err)

}

func Del(ctx context.Context, param ...string) (string, error) {
	if len(param) != 1 {
		return "", config.ErrInvalidParam
	}

	grpcClient, err := Client()
	if err != nil {
		return "", err
	}
	resp, err := grpcClient.Del(ctx, &proto.DelRequest{
		Key:          param[0],
		RequiredAcks: ack,
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

	grpcClient, err := Client()
	if err != nil {
		return "", err
	}
	resp, err := grpcClient.Expire(ctx, &proto.ExpireRequest{
		Key:          param[0],
		Ttl:          uint32(ttl),
		RequiredAcks: ack,
	})
	return handleBaseResponse(resp, err)

}

func TTL(ctx context.Context, param ...string) (string, error) {
	if len(param) != 1 {
		return "", config.ErrInvalidParam
	}

	grpcClient, err := Client()
	if err != nil {
		return "", err
	}
	resp, err := grpcClient.TTL(ctx, &proto.TTLRequest{
		Key: param[0],
	})
	return handleBaseResponse(resp, err)

}
