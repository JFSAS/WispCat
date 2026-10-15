#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ "$(uname -s)" = Darwin ]; then
  openssl_prefix="$(brew --prefix openssl@3)"
  export CGO_CFLAGS="${CGO_CFLAGS:-} -I${openssl_prefix}/include"
  export CGO_LDFLAGS="${CGO_LDFLAGS:-} -L${openssl_prefix}/lib"
fi
exec go "$@"
