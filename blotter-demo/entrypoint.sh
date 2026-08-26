#!/usr/bin/env bash
# Demo entrypoint: wait for QuestDB, create the base table + LIVE VIEW, start the
# synthetic feed in the background, then run the blotter in the foreground (TTY).
set -euo pipefail

HTTP="${QDB_HTTP:-questdb:9000}"
ADDR="${QDB_ADDR:-questdb:9000}"

exec_sql() {
    # URL-encodes the query via curl --data-urlencode; fails the script on HTTP error.
    curl -fsS -G "http://${HTTP}/exec" --data-urlencode "query=$1" -o /dev/null
}

echo "[entrypoint] waiting for QuestDB at ${HTTP} ..."
until curl -fsS "http://${HTTP}/exec?query=select%201" -o /dev/null 2>/dev/null; do
    sleep 1
done
echo "[entrypoint] QuestDB is up"

echo "[entrypoint] creating base table core_price_demo"
exec_sql "CREATE TABLE IF NOT EXISTS core_price_demo (
             timestamp TIMESTAMP,
             symbol SYMBOL,
             bid_price DOUBLE
         ) TIMESTAMP(timestamp) PARTITION BY DAY WAL;"

echo "[entrypoint] creating LIVE VIEW core_price_lv"
# A live view serves its results from memory, so ingested rows are visible
# immediately; FLUSH EVERY only controls how often results are committed to disk
# and does not affect what the blotter sees. Visible motion comes from the feed
# flushing to the base table often (feed.py flushes 20x/s).
exec_sql "CREATE LIVE VIEW IF NOT EXISTS core_price_lv
          FLUSH EVERY 5s IN MEMORY 5s START FROM NOW AS
          SELECT
              timestamp,
              symbol,
              bid_price,
              avg(bid_price) OVER w AS moving_avg
          FROM core_price_demo
          WINDOW w AS (PARTITION BY symbol ORDER BY timestamp ANCHOR DAILY '00:00');"

echo "[entrypoint] starting synthetic feed"
python /app/feed.py &
FEED_PID=$!
trap 'kill "${FEED_PID}" 2>/dev/null || true' EXIT INT TERM

# Give the feed a moment so the first blotter frame is not empty.
sleep 2

echo "[entrypoint] starting blotter (Ctrl+C to quit)"
# Query the live view directly (the moving-average view), newest rows at the tail.
exec python /app/blotter.py \
    --table core_price_lv \
    --limit -20 \
    --rate 10 \
    --addr "${ADDR}"
