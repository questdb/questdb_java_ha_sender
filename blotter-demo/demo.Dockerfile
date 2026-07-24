# Blotter demo app: the unreleased questdb 5.0 Python client (built from source),
# plus the feed + blotter scripts. Built once, then runs with zero host deps.
#
# The client is pinned by SHA. NOTE: unlike the questdb server commit, this branch
# (jh_experiment_new_ilp) may be DELETED before it merges, which would make the SHA
# unfetchable from GitHub. If that happens, run blotter-demo/vendor.sh to snapshot the
# source locally and switch the client-build stage to COPY it in (see vendor.sh).
ARG CLIENT_COMMIT=ea54b6f474062c144aa6395facad42e77c99e6f6
ARG RUST_VERSION=1.91.1

FROM python:3.12-slim AS client-build
ARG CLIENT_COMMIT
ARG RUST_VERSION
RUN apt-get update && apt-get install -y --no-install-recommends \
        git curl build-essential pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# Rust >= 1.91.1: the bundled c-questdb-client (questdb-rs 7.0.0) requires it.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --default-toolchain "${RUST_VERSION}" --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /src
RUN git clone https://github.com/questdb/py-questdb-client.git .
RUN git checkout "${CLIENT_COMMIT}"
RUN git submodule update --init --recursive
# Build-time deps must stay in the isolated build env (do NOT use
# --no-build-isolation; that pins the wrong numpy and fails the cython compile).
RUN pip install --no-cache-dir -U pip "cython>=3.1.2" "setuptools>=80.9.0" numpy
RUN pip wheel --no-deps --no-cache-dir -w /wheels .

FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=client-build /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl polars pyarrow

WORKDIR /app
COPY python/blotter.py /app/blotter.py
COPY blotter-demo/feed.py /app/feed.py
COPY blotter-demo/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENV QDB_ADDR=questdb:9000 \
    QDB_HTTP=questdb:9000
ENTRYPOINT ["/app/entrypoint.sh"]
