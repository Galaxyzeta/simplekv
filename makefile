gen:
	protoc --go_out=plugins=grpc:. proto/simplekv.proto
	protoc --go_out=plugins=grpc:. proto/controlplane.proto
	protoc --go_out=plugins=grpc:. proto/base.proto
standalone:
	go run ./server -cfg "conf/standalone/server-standalone.yaml"
cluster3:
	go run ./server -cfg "conf/cluster3/server-node01.yaml"
	go run ./server -cfg "conf/cluster3/server-node02.yaml"
	go run ./server -cfg "conf/cluster3/server-node03.yaml"
cli:
	go run ./client-main -cfg "conf/client/cli.yaml"
cluster2-1:
	go run ./server -cfg "conf/cluster2/server-node01.yaml"
cluster2-2:
	go run ./server -cfg "conf/cluster2/server-node02.yaml"
cluster3-1:
	go run ./server -cfg "conf/cluster3/server-node01.yaml"
cluster3-2:
	go run ./server -cfg "conf/cluster3/server-node02.yaml"
cluster3-3:
	go run ./server -cfg "conf/cluster3/server-node03.yaml"