#!/bin/sh
# Prepared Go dependencies and Python standard library only; no external tests.
set -eu
cd "$(dirname "$0")/.."
export GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOENV=off GOTELEMETRY=off
mkdir -p .bin
mcp_source_sha="$(python3 tests/mcp/source_capture.py)"
sh tools/go.sh build -race -tags integrationfixtures -trimpath -ldflags "-X main.deploymentSourceSHA=$mcp_source_sha" -o .bin/mcp-host-fixture ./tests/deployment/fixture
sh tools/go.sh build -race -trimpath -o .bin/mcp-stream-client ./cmd/wechat-mcp
python3 tests/mcp/source_capture.py --check
case "$(uname -s)" in
  Darwin)
    command -v sandbox-exec >/dev/null
    sandbox-exec -p '(version 1)(allow default)(deny network-outbound)(allow network-outbound (remote ip "localhost:*"))' python3 tests/mcp/stdio_probe.py
    ;;
  Linux)
    sh tools/require-loopback-network.sh
    python3 tests/mcp/stdio_probe.py
    ;;
  *) echo 'No enforced offline network sandbox for this OS.' >&2; exit 1 ;;
esac
