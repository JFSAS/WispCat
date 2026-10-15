#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
export GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local
python3 tools/check_baseline.py
python3 tools/test-baseline-checker.py
if [ "$(uname -s)" = Darwin ] && command -v sandbox-exec >/dev/null 2>&1; then
  sandbox-exec -p '(version 1)(allow default)(deny network-outbound)(allow network-outbound (remote ip "localhost:*"))' sh tools/go.sh test -race -count=1 ./...
else
  sh tools/go.sh test -race -count=1 ./...
fi
sh tools/go.sh vet ./...
sh tools/go.sh build -trimpath ./cmd/wechat-runtime ./cmd/wechat-mcp ./examples/echo
git diff --check
