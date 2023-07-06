#!/bin/bash

set -e

# check and download dependency for gRPC code generate
if [ ! -e ./proto/vendor/protobuf/src/google/protobuf ]; then
    rm -rf ./proto/vendor/protobuf/src/google/protobuf
    DIR="./proto/vendor/protobuf/src/google/protobuf"
    mkdir -p $DIR
    wget https://raw.githubusercontent.com/protocolbuffers/protobuf/v3.9.0/src/google/protobuf/empty.proto -P $DIR
fi

# proto lint check
#buf check lint

PROTO_DIR="proto/spdkrpc"
TMP_DIR_BASE=".protobuild"
TMP_DIR="${TMP_DIR_BASE}/github.com/longhorn/longhorn-spdk-engine/proto/"
mkdir -p "${TMP_DIR}"
cp -a "${PROTO_DIR}"/*.proto "${TMP_DIR}"
for PROTO in spdk; do
#    mkdir -p "integration/rpc/${PROTO}"
#    python3 -m grpc_tools.protoc -I "${TMP_DIR_BASE}" -I "proto/vendor/" -I "proto/vendor/protobuf/src/" --python_out=integration/rpc/${PROTO} --grpc_python_out=integration/rpc/${PROTO} "${TMP_DIR}/${PROTO}.proto"
    protoc -I ${TMP_DIR_BASE}/ -I proto/vendor/ -I proto/vendor/protobuf/src/ "${TMP_DIR}/${PROTO}.proto" --go_out=plugins=grpc:"${TMP_DIR_BASE}"
    mv "${TMP_DIR}/${PROTO}.pb.go" "${PROTO_DIR}/${PROTO}.pb.go"

#    mv integration/rpc/${PROTO}/github.com/longhorn/longhorn_spdk_engine/proto/${PROTO}_pb2_grpc.py integration/rpc/${PROTO}/github/com/longhorn/longhorn_spdk_engine/proto/${PROTO}_pb2_grpc.py
#    rm -rf integration/rpc/${PROTO}/github.com
done

rm -rf "${TMP_DIR_BASE}"
