gen:
	protoc --go_out=plugins=grpc:. service/proto/simplekv.proto
server:
	go run ./service/server -cfg "config/server-default.yaml"
client:
	go run ./service/client