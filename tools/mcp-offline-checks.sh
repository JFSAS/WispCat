#!/bin/sh
# Prepared dependencies only; SDK mode additionally needs the pinned Python SDK.
set -eu
cd "$(dirname "$0")/.."
export GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOENV=off GOTELEMETRY=off
mkdir -p .bin
mcp_python="${MCP_PYTHON:-python3}"
mcp_probe=tests/mcp/stdio_probe.py
case "${1:-}" in '' ) ;; --sdk) mcp_probe=tests/mcp/sdk_probe.py ;; *) echo 'Unknown MCP probe.' >&2; exit 1 ;; esac
mcp_source_sha="$("$mcp_python" tests/mcp/source_capture.py)"
sh tools/go.sh build -race -tags integrationfixtures -trimpath -ldflags "-X main.deploymentSourceSHA=$mcp_source_sha" -o .bin/mcp-host-fixture ./tests/deployment/fixture
sh tools/go.sh build -race -trimpath -o .bin/mcp-stream-client ./cmd/wispcat-mcp
"$mcp_python" tests/mcp/source_capture.py --check
case "$(uname -s)" in
  Darwin)
    command -v sandbox-exec >/dev/null
    if [ "$mcp_probe" = tests/mcp/sdk_probe.py ]; then
      sandbox-exec -p '(version 1)(allow default)(deny network-outbound)(allow network-outbound (remote ip "localhost:*"))' "$mcp_python" -m unittest discover -s tests/mcp -p test_probe_cleanup.py
    fi
    sandbox-exec -p '(version 1)(allow default)(deny network-outbound)(allow network-outbound (remote ip "localhost:*"))' "$mcp_python" "$mcp_probe"
    ;;
  Linux)
    sh tools/require-loopback-network.sh
    if [ "$mcp_probe" = tests/mcp/sdk_probe.py ]; then
      "$mcp_python" -m unittest discover -s tests/mcp -p test_probe_cleanup.py
    fi
    "$mcp_python" "$mcp_probe"
    ;;
  *) echo 'No enforced offline network sandbox for this OS.' >&2; exit 1 ;;
esac
