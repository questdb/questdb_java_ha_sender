#!/usr/bin/env bash
#
# Regenerate the replay CSV from QuestDB's public demo instance.
#
# Pulls the most recent trades from https://demo.questdb.io via the /exp CSV
# export endpoint, so the sender replays fresh data instead of a stale snapshot.
#
# Usage:
#   ./regenerate_csv.sh [output-file] [limit]
#
#   output-file  destination path (default: trades.csv). If it ends in .gz the
#                result is gzip-compressed, matching the sender's default
#                --csv ./trades20250728.csv.gz (loadCsv auto-detects .gz).
#   limit        number of most-recent rows to fetch (default: 1000000).
#
# Env overrides:
#   DEMO_HOST    base URL (default: https://demo.questdb.io)
#
# Examples:
#   ./regenerate_csv.sh                      # -> trades.csv, 1,000,000 rows
#   ./regenerate_csv.sh trades.csv.gz        # -> gzipped
#   ./regenerate_csv.sh trades.csv 250000    # -> 250k rows
set -euo pipefail

OUT="${1:-trades.csv}"
LIMIT="${2:-1000000}"
DEMO_HOST="${DEMO_HOST:-https://demo.questdb.io}"

# select * gives symbol, side, price, amount, timestamp: exactly the columns the
# sender reads (timestamp only needed with --timestamp-from-file). Ordering desc
# gives the most recent rows; the sender stamps now() by default so order does
# not affect ingestion unless --timestamp-from-file is set.
QUERY="select * from trades order by timestamp desc limit ${LIMIT}"

# Download to a temp file first so a failed/partial fetch never clobbers a good CSV.
TMP="$(mktemp "${TMPDIR:-/tmp}/trades.XXXXXX.csv")"
trap 'rm -f "$TMP"' EXIT

echo "Fetching ${LIMIT} rows from ${DEMO_HOST}/exp ..."
# -G + --data-urlencode builds the encoded query string; --fail turns HTTP errors
# into a non-zero exit; --retry rides out transient network blips.
curl --fail --show-error --silent --retry 3 --retry-delay 1 --retry-all-errors \
  -G "${DEMO_HOST}/exp" \
  --data-urlencode "query=${QUERY}" \
  -o "$TMP"

# Sanity: a valid export has a header line plus data.
LINES="$(wc -l < "$TMP" | tr -d ' ')"
if [ "$LINES" -lt 2 ]; then
  echo "ERROR: export returned only ${LINES} line(s); leaving ${OUT} untouched." >&2
  echo "Response was:" >&2
  head -c 500 "$TMP" >&2
  exit 1
fi

case "$OUT" in
  *.gz)
    gzip -c "$TMP" > "$OUT"
    ;;
  *)
    cp "$TMP" "$OUT"
    ;;
esac

echo "Wrote ${OUT} ($((LINES - 1)) data rows, header included)."
