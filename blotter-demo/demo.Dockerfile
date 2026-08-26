# Blotter demo app: the questdb 5.0 Python client (from PyPI) plus the feed +
# blotter scripts. Built once, then runs with zero host deps.
#
# The client shipped as 5.0.0 stable, so there is no source build anymore — the
# query API (questdb.connect(...).query(sql).to_polars()) that the blotter uses is
# in the released wheel. (Before 5.0.0 this stage compiled the client from an
# unreleased branch with a full Rust + Cython toolchain.)
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir questdb==5.0.0 polars pyarrow

WORKDIR /app
COPY python/blotter.py /app/blotter.py
COPY blotter-demo/feed.py /app/feed.py
COPY blotter-demo/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENV QDB_ADDR=questdb:9000 \
    QDB_HTTP=questdb:9000
ENTRYPOINT ["/app/entrypoint.sh"]
