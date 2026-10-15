#!/bin/sh
# Run inside a Linux namespace/container with dependencies already available.
# The caller supplies a task-local Go toolchain/sysroot when the host has none.
set -eu
cd "$(dirname "$0")/.."
export GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOENV=off CGO_ENABLED=1
test "$(uname -s)" = Linux
if [ -n "$(ip -o address show scope global)$(ip route show)$(ip -6 route show default)" ]; then
  echo 'Checks require a loopback-only Linux network namespace.' >&2
  exit 1
fi
python3 tools/check_baseline.py
python3 tools/test-baseline-checker.py
ip -brief address
ip route show
go version
pkg-config --modversion openssl sqlite3
mkdir -p .bin
go test -race -count=1 -json ./... > .bin/linux-go-test.jsonl
python3 tools/report-go-tests.py .bin/linux-go-test.jsonl --report .bin/linux-test-summary.json
go vet ./...
mkdir -p .bin
go build -trimpath -o .bin/wechat-runtime ./cmd/wechat-runtime
go build -trimpath -o .bin/wechat-mcp ./cmd/wechat-mcp
go version -m .bin/wechat-runtime
ldd .bin/wechat-runtime
python3 tools/offline-process-smoke.py --binary .bin/wechat-runtime --report .bin/linux-process-smoke.json
