PROTO_DIR = api/proto/v1
PROTO_NAME = todo-service.proto
PROTO_OUT_DIR = pkg/api/v1

all: protoc-grpc

print:
	echo $(PROTO_DIR)

protoc-grpc:
	protoc -I/usr/local/include \
		-I. \
		-I./$(PROTO_DIR) \
		-I${GOPATH}/src \
		-I${GOPATH}/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
		-I${GOPATH}/src/github.com/protocolbuffers/protobuf/src/google/protobuf \
		-Ithird_party/googleapis \
		--go_out=plugins=grpc:$(PROTO_OUT_DIR) \
		$(PROTO_NAME)