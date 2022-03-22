gen:
	protoc --go_out=plugins=grpc:. proto/simplekv.proto
	protoc --go_out=plugins=grpc:. proto/controlplane.proto
	protoc --go_out=plugins=grpc:. proto/base.proto
standalone:
	go run ./server -cfg "conf/server-standalone.yaml"
cluster3:
	go run ./server -cfg "conf/cluster3/server-node01.yaml"
	go run ./server -cfg "conf/cluster3/server-node02.yaml"
	go run ./server -cfg "conf/cluster3/server-node03.yaml"
cli:
	go run ./client -cfg "client/conf/cluster2.yaml"
cluster2-1:
	go run ./server -cfg "conf/cluster2/server-node01.yaml"
cluster2-2:
	go run ./server -cfg "conf/cluster2/server-node02.yaml"
