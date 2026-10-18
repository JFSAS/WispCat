#!/bin/sh
# Bounded Alpha acceptance checks; dependencies must already be available.
set -eu
cd "$(dirname "$0")/.."
export GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOENV=off GOTELEMETRY=off

run_offline() {
  case "$(uname -s)" in
    Darwin)
      command -v sandbox-exec >/dev/null
      sandbox-exec -p '(version 1)(allow default)(deny network-outbound)(allow network-outbound (remote ip "localhost:*"))' "$@"
      ;;
    Linux)
      sh tools/require-loopback-network.sh
      "$@"
      ;;
    *) echo 'Alpha checks require an enforced macOS or Linux offline environment.' >&2; exit 1 ;;
  esac
}

python3 tools/check_baseline.py
python3 tools/test-baseline-checker.py
python3 tools/test-release.py
run_offline sh tools/go.sh test -run '^$' ./...
run_offline sh tools/go.sh test -count=1 \
  ./cmd/wispcat-credentials ./internal/credentials ./internal/host \
  ./internal/onebot ./internal/mcp ./internal/securedb ./pkg/client ./internal/catalog
run_offline sh tools/go.sh test -count=1 \
  -run '^(TestCredential|TestSerialWorkerCancelsCredentialRPCAndJoins|TestDisk|TestExplicitDiskLogin|TestCacheResume|TestCacheRecovery|TestNetworkOutage|TestRecoveryWorker|TestSaveRecovery|TestSyncProtocol|TestCertificateFailure|TestFailedSendJournal|TestFailedSyncPreserves|TestFailureDiagnostics|TestPermanentProtocol|TestExpiredSession|TestHTTPOutage)' \
  ./internal/service
run_offline sh tools/go.sh test -count=1 -run '^TestHTTPResponse' ./internal/protocol
sh tools/go.sh vet ./...
mkdir -p .bin
sh tools/go.sh build -trimpath -o .bin/wispcat ./cmd/wispcat
sh tools/go.sh build -trimpath -o .bin/wispcat-mcp ./cmd/wispcat-mcp
sh tools/go.sh build -trimpath -o .bin/wispcat-credentials ./cmd/wispcat-credentials
sh tools/go.sh build -trimpath -o .bin/echo ./examples/echo
run_offline python3 tools/offline-process-smoke.py --binary .bin/wispcat --report /dev/null
git diff --check
