#!/bin/sh
# Prepared Python dependencies only; fixture/consumer processes cannot go online.
set -eu
cd "$(dirname "$0")/.."
export GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOENV=off
python="${DEPLOYMENT_PYTHON:-.bin/deployment-env/bin/python}"
test -x "$python" || { echo 'Prepare the isolated deployment environment first.' >&2; exit 1; }
mkdir -p .bin
deployment_scenario=text
case "${1:-}" in --media) deployment_scenario=media; shift ;; --large-media) deployment_scenario=large-media; shift ;; esac
deployment_source_sha="$("$python" tests/deployment/source_capture.py)"
sh tools/go.sh build -race -tags integrationfixtures -trimpath -ldflags "-X main.deploymentSourceSHA=$deployment_source_sha" -o .bin/deployment-fixture ./tests/deployment/fixture
"$python" tests/deployment/source_capture.py --check
if [ "$#" -eq 0 ]; then set -- nonebot astrbot; fi
for framework do
  case "$framework" in nonebot|astrbot|astrbot-application) ;; *) echo 'Unknown deployment peer.' >&2; exit 1 ;; esac
  case "$(uname -s)" in
    Darwin)
      command -v sandbox-exec >/dev/null
      if [ "$framework" = astrbot-application ]; then
        sandbox-exec -p '(version 1)(allow default)(deny network-outbound)(allow network-outbound (remote ip "localhost:*"))' "$python" -m unittest discover -s tests/deployment -p test_application_audit.py
      fi
      sandbox-exec -p '(version 1)(allow default)(deny network-outbound)(allow network-outbound (remote ip "localhost:*"))' "$python" tests/deployment/run.py "$framework" "$deployment_scenario"
      ;;
    Linux)
      sh tools/require-loopback-network.sh
      if [ "$framework" = astrbot-application ]; then
        "$python" -m unittest discover -s tests/deployment -p test_application_audit.py
      fi
      "$python" tests/deployment/run.py "$framework" "$deployment_scenario"
      ;;
    *) echo 'No enforced offline network sandbox for this OS.' >&2; exit 1 ;;
  esac
done
