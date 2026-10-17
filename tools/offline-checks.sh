#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
export GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local
python3 tools/check_baseline.py
python3 tools/test-baseline-checker.py
python3 tools/test-offline-evidence.py
if [ "$(uname -s)" = Darwin ] && command -v sandbox-exec >/dev/null 2>&1; then
  sandbox-exec -p '(version 1)(allow default)(deny network-outbound)(allow network-outbound (remote ip "localhost:*"))' python3 tools/test-container-probes.py
  sandbox-exec -p '(version 1)(allow default)(deny network-outbound)(allow network-outbound (remote ip "localhost:*"))' sh tools/go.sh test -race -count=1 ./...
else
  sh tools/require-loopback-network.sh
  python3 tools/test-container-probes.py
  sh tools/go.sh test -race -count=1 ./...
fi
sh tools/go.sh vet ./...
sh tools/go.sh build -trimpath ./cmd/wispcat ./cmd/wispcat-mcp ./cmd/wispcat-credentials ./examples/echo
git diff --check
