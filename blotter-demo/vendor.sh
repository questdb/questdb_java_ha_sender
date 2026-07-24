#!/usr/bin/env bash
# Fallback for when the client branch is deleted before it merges: snapshot the
# py-questdb-client source (at the pinned commit, submodules included) into a local
# tarball so the Docker build no longer depends on GitHub keeping the SHA reachable.
#
# Usage:
#   ./vendor.sh                       # snapshots CLIENT_COMMIT below into ./vendor/
# Then edit demo.Dockerfile's client-build stage to use the tarball instead of the
# git clone/checkout (a COPY + tar -xzf), and rebuild. Instructions printed at the end.
set -euo pipefail

CLIENT_COMMIT="${CLIENT_COMMIT:-ea54b6f474062c144aa6395facad42e77c99e6f6}"
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="${HERE}/vendor"
WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

mkdir -p "${OUT}"
echo "[vendor] cloning py-questdb-client @ ${CLIENT_COMMIT} ..."
git clone https://github.com/questdb/py-questdb-client.git "${WORK}/src"
git -C "${WORK}/src" checkout "${CLIENT_COMMIT}"
git -C "${WORK}/src" submodule update --init --recursive

TARBALL="${OUT}/py-questdb-client-${CLIENT_COMMIT:0:12}.tar.gz"
echo "[vendor] writing ${TARBALL}"
# Exclude .git to keep the tarball small; source + submodules are enough to build.
tar --exclude-vcs -czf "${TARBALL}" -C "${WORK}/src" .

cat <<EOF

[vendor] done -> ${TARBALL}

To use it, replace the git clone/checkout/submodule lines in the client-build
stage of blotter-demo/demo.Dockerfile with:

    COPY blotter-demo/vendor/py-questdb-client-${CLIENT_COMMIT:0:12}.tar.gz /tmp/client.tar.gz
    RUN mkdir -p /src && tar -xzf /tmp/client.tar.gz -C /src

Then rebuild: docker compose build demo
EOF
