#!/bin/sh
# Prepared Python dependencies only; fixture/consumer processes cannot go online.
set -eu
cd "$(dirname "$0")/.."
export GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOENV=off
python="${DEPLOYMENT_PYTHON:-.bin/deployment-env/bin/python}"
test -x "$python" || { echo 'Prepare the isolated deployment environment first.' >&2; exit 1; }
mkdir -p .bin
deployment_scenario=text
if [ "${1:-}" = --media ]; then deployment_scenario=media; shift; fi
deployment_source_sha="$("$python" tests/deployment/source_capture.py)"
sh tools/go.sh build -race -tags integrationfixtures -trimpath -ldflags "-X main.deploymentSourceSHA=$deployment_source_sha" -o .bin/deployment-fixture ./tests/deployment/fixture
"$python" tests/deployment/source_capture.py --check
if [ "$#" -eq 0 ]; then set -- nonebot astrbot; fi
for framework do
  case "$framework" in nonebot|astrbot) ;; *) echo 'Unknown deployment peer.' >&2; exit 1 ;; esac
  case "$(uname -s)" in
    Darwin)
      command -v sandbox-exec >/dev/null
      sandbox-exec -p '(version 1)(allow default)(deny network-outbound)(allow network-outbound (remote ip "localhost:*"))' "$python" tests/deployment/run.py "$framework" "$deployment_scenario"
      ;;
    Linux)
      sh tools/require-loopback-network.sh
      "$python" tests/deployment/run.py "$framework" "$deployment_scenario"
      ;;
    *) echo 'No enforced offline network sandbox for this OS.' >&2; exit 1 ;;
  esac
done
