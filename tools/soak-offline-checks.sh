#!/bin/sh
# Timed synthetic native media workers + real local BotAdapter/OneBot/SDK paths.
# No login, account import or production protocol session worker is started.
set -eu
cd "$(dirname "$0")/.."
export GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOENV=off
export WECHAT_SOAK_DURATION="${WECHAT_SOAK_DURATION:-10m}"
mkdir -p .bin/reports
case "$(uname -s)" in
  Darwin)
    platform=macos
    command -v sandbox-exec >/dev/null
    ;;
  Linux)
    platform=linux
    sh tools/require-loopback-network.sh
    ;;
  *) echo 'No enforced offline network sandbox for this OS.' >&2; exit 1 ;;
esac
soak_run=$(mktemp -d ".bin/$platform-runtime-soak.XXXXXX")
manifest="$soak_run/source.json"
python3 tools/report-runtime-soak.py capture "$manifest"
soak_source_sha=$(python3 -c 'import hashlib,sys; from pathlib import Path; print(hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest())' "$manifest")
soak_ldflags="-X github.com/JFSAS/wispcat/internal/service.soakSourceManifestSHA=$soak_source_sha"
sh tools/go.sh vet -tags runtimefixtures ./internal/service
if [ "$platform" = macos ]; then
  sandbox-exec -p '(version 1)(allow default)(deny network-outbound)(allow network-outbound (remote ip "localhost:*"))' sh tools/go.sh test -race -count=1 -timeout 70m -json -tags runtimefixtures -ldflags "$soak_ldflags" ./internal/service -run '^TestRuntimeOfflineSoak$' > "$soak_run/tests.jsonl"
else
  sh tools/go.sh test -race -count=1 -timeout 70m -json -tags runtimefixtures -ldflags "$soak_ldflags" ./internal/service -run '^TestRuntimeOfflineSoak$' > "$soak_run/tests.jsonl"
fi
python3 tools/report-runtime-soak.py summarize "$soak_run/tests.jsonl" --manifest "$manifest" --report ".bin/reports/$platform-runtime-soak.json"
