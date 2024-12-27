# Build proto files

.PHONY: proto run

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
	        --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	        protos/database.proto

run:
    go run ./