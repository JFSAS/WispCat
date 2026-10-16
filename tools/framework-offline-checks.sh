#!/bin/sh
# Install locked dependencies/source separately before running this script.
set -eu
cd "$(dirname "$0")/.."
export GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOENV=off
python="${FRAMEWORK_PYTHON:-.bin/framework-env/bin/python}"
test -x "$python" || { echo 'Prepare the isolated framework environment first.' >&2; exit 1; }
mkdir -p .bin
CGO_ENABLED=0 go build -tags integrationfixtures -o .bin/framework-fixture ./tests/framework/fixture
if [ "$#" -eq 0 ]; then
  set -- nonebot aiocqhttp astrbot
fi
for framework do
  case "$framework" in nonebot|aiocqhttp|astrbot) ;; *) echo 'Unknown framework.' >&2; exit 1 ;; esac
  case "$(uname -s)" in
    Darwin)
      command -v sandbox-exec >/dev/null
      sandbox-exec -p '(version 1)(allow default)(deny network-outbound)(allow network-outbound (remote ip "localhost:*"))' "$python" tests/framework/run.py "$framework"
      ;;
    Linux)
      # A namespace with only loopback must be established by the caller.
      sh tools/require-loopback-network.sh
      "$python" tests/framework/run.py "$framework"
      ;;
    *) echo 'No enforced offline network sandbox for this OS.' >&2; exit 1 ;;
  esac
done
