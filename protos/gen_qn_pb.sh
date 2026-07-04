#!/usr/bin/env bash
# Regenerate the QuickNode Hypercore gRPC stubs from protos/orderbook.proto
# into src/hyperliquid_pipeline/collectors/_qn_pb/.
#
# protoc emits a top-level `import orderbook_pb2` in the grpc stub; the sed
# below rewrites it package-relative so the stubs work inside the package.
# (sed -i '' is the macOS spelling; on Linux use sed -i.)
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=src/hyperliquid_pipeline/collectors/_qn_pb
mkdir -p "$OUT"

.venv/bin/python -m grpc_tools.protoc -I protos \
  --python_out="$OUT" --grpc_python_out="$OUT" protos/orderbook.proto

sed -i '' 's/^import orderbook_pb2 as orderbook__pb2$/from . import orderbook_pb2 as orderbook__pb2/' \
  "$OUT/orderbook_pb2_grpc.py"

echo "generated: $OUT/orderbook_pb2.py, $OUT/orderbook_pb2_grpc.py"
