# Build proto files

.PHONY: proto lint

protodb:
	protoc --go_out=. --go_opt=paths=source_relative \
	        --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	        protos/BagginsDBService.proto

lint:
	-staticcheck ./... || exit 0

dummyGet:
	grpcurl \
	-plaintext \
	-d '{"partition_key": "my_partition_key", "clustering_keys": ["cluster_key_1", "cluster_key_2"], "column_name": "my_column_name"}' \
	localhost:8082 \
	bagginsdb.BagginsDBService/HandleRequest


dummySet:
	grpcurl \
	-plaintext \
	-d '{"partition_key": "my_partition_key", "clustering_keys": ["cluster_key_1", "cluster_key_2"], "column_name": "my_column_name", "value": "my_value"}' \
	localhost:8082 \
	bagginsdb.BagginsDBService/HandleRequest



