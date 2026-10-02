#!/usr/bin/env bash
#
# Regenerate the replay CSV from the FX table on QuestDB's public demo instance.
#
# Companion to regenerate_csv.sh (which pulls the crypto `trades` table). This one
# pulls `fx_trades`, so you replay FX symbols (EUR-USD, ...) instead of crypto.
#
# We ALWAYS export from the demo box (https://demo.questdb.io): it is continuously
# ingesting, so every export is a fresh, recent-price snapshot. You then recreate
# the table locally from the CSV for internal demos.
#
# Two schema notes about fx_trades vs the sender's fixed `trades` schema:
#   * Its size column is `quantity`, so the query aliases `quantity AS amount` to
#     match the `amount` column the sender writes.
#   * Its designated `timestamp` is TIMESTAMP_NS (nanoseconds), where the crypto
#     `trades` table is micros. This only matters with --timestamp-from-file; see
#     the README ("Nanosecond vs microsecond timestamps"). In the default replay
#     mode the sender stamps now() and ignores the file timestamp entirely.
#
# Usage:
#   ./regenerate_csv_fx.sh [output-file] [limit]
#
#   output-file  destination path (default: fx_trades.csv). If it ends in .gz the
#                result is gzip-compressed (loadCsv auto-detects .gz).
#   limit        number of most-recent rows to fetch (default: 1000000).
#
# Env overrides:
#   DEMO_HOST    base URL (default: https://demo.questdb.io). Intentionally the
#                demo box; override only if you mirror fx_trades elsewhere.
#
# Examples:
#   ./regenerate_csv_fx.sh                       # -> fx_trades.csv, 1,000,000 rows
#   ./regenerate_csv_fx.sh fx_trades.csv.gz      # -> gzipped
#   ./regenerate_csv_fx.sh fx_trades.csv 250000  # -> 250k rows
set -euo pipefail

OUT="${1:-fx_trades.csv}"
LIMIT="${2:-1000000}"
DEMO_HOST="${DEMO_HOST:-https://demo.questdb.io}"

# Explicit column list (not select *) so we can alias quantity -> amount and drop
# the fx-only columns (ecn, counterparty, order_id, ...) the sender does not read.
# The result has exactly symbol, side, price, amount, timestamp: the sender's schema.
QUERY="select timestamp, symbol, side, price, quantity as amount from fx_trades order by timestamp desc limit ${LIMIT}"

# Download to a temp file first so a failed/partial fetch never clobbers a good CSV.
TMP="$(mktemp "${TMPDIR:-/tmp}/fx_trades.XXXXXX.csv")"
trap 'rm -f "$TMP"' EXIT

echo "Fetching ${LIMIT} rows from ${DEMO_HOST}/exp (fx_trades) ..."
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
