#!/bin/sh
# Synthetic production submission/checkpoint/archive capacity, no worker login.
# Prepare dependencies first; Linux callers must provide a loopback-only namespace.
set -eu
cd "$(dirname "$0")/.."
export GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOENV=off
mkdir -p .bin
case "$(uname -s)" in
  Darwin)
    platform=macos
    command -v sandbox-exec >/dev/null
    sandbox-exec -p '(version 1)(allow default)(deny network-outbound)(allow network-outbound (remote ip "localhost:*"))' sh tools/go.sh test -race -count=1 -timeout 30m -json -tags storagefixtures ./internal/service -run '^TestArchiveCumulativeCapacity$' > ".bin/$platform-archive-load.jsonl"
    ;;
  Linux)
    platform=linux
    if [ -n "$(ip -o address show scope global)$(ip route show)$(ip -6 route show default)" ]; then
      echo 'Archive checks require a loopback-only Linux network namespace.' >&2
      exit 1
    fi
    sh tools/go.sh test -race -count=1 -timeout 30m -json -tags storagefixtures ./internal/service -run '^TestArchiveCumulativeCapacity$' > ".bin/$platform-archive-load.jsonl"
    ;;
  *) echo 'No enforced offline network sandbox for this OS.' >&2; exit 1 ;;
esac
python3 tools/report-go-tests.py ".bin/$platform-archive-load.jsonl" --report ".bin/$platform-archive-load-summary.json"
