#!/usr/bin/env bash
# run_blotter.sh - launch the live blotter with your standard address + token.
#
# Set the token the same way as the senders, then run; extra args pass straight through
# to blotter.py (table, --where, --limit, --rate, ...):
#
#   export ILP_TOKEN=<your token>
#   ./run_blotter.sh core_price_lv --where "symbol = 'EURUSD'" --limit -20 --rate 5
#
# Override the address or the python interpreter via env vars when needed:
#   ADDR="host:9000"          # default is the demo cluster below (comma-separated = failover)
#   PYTHON="/path/to/venv/bin/python"   # the interpreter that has the QWP client
#
# Run it (bash run_blotter.sh ...), don't source it.

ADDR="${ADDR:-172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000}"
PYTHON="${PYTHON:-python3}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -z "${ILP_TOKEN:-}" ]; then
  echo "ILP_TOKEN is not set. Run:  export ILP_TOKEN=<your token>" >&2
  exit 1
fi

"$PYTHON" "$DIR/blotter.py" \
  --addr "$ADDR" \
  --token "$ILP_TOKEN" \
  --tls-verify unsafe_off \
  "$@"
