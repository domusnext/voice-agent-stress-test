#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PROTO_DIR="$SCRIPT_DIR"
OUT_DIR="$PROJECT_DIR/src/proto_generated"

mkdir -p "$OUT_DIR"
touch "$OUT_DIR/__init__.py"

python -m grpc_tools.protoc \
    -I "$PROTO_DIR" \
    --python_out="$OUT_DIR" \
    --grpc_python_out="$OUT_DIR" \
    "$PROTO_DIR/voice_agent_transport.proto"

# 修复 grpc 生成代码的 import 路径（bare import → package import）
sed -i '' 's/^import voice_agent_transport_pb2/import proto_generated.voice_agent_transport_pb2/' \
    "$OUT_DIR/voice_agent_transport_pb2_grpc.py"

echo "Proto 代码已生成到 $OUT_DIR"
