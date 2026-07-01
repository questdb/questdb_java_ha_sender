#!/usr/bin/env bash
# qdb-primary-watchdog.sh
#
# Watches the primary's health. After N consecutive failures it promotes the LOCAL replica
# to primary with a single authenticated REST call to the lifecycle switch endpoint, then
# polls /lifecycle until the async switch has actually completed.
#
# The new failover mechanism makes this trivial: no stopping processes, no marker files,
# no env-var juggling, no restarting the instance, no sudo. Just health-ping + one POST.
#
# Assumptions:
#   - This script runs ON the replica you want promoted (SWITCH_URL points at localhost).
#   - QDB_REST_TOKEN is set (bearer token for the lifecycle endpoint).
# If you run it elsewhere, point SWITCH_URL / LIFECYCLE_URL at the replica to promote.

# Do NOT source this script
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  echo "Please RUN this script, do not source it. Try: bash ${BASH_SOURCE[0]}"
  return 1 2>/dev/null || exit 1
fi

set -o pipefail
LC_ALL=C

# ===================== Config =====================
PRIMARY_HEALTH_URL="http://172.31.42.41:9003/lifecycle"  # remote primary to watch (health ping)
SWITCH_URL="http://localhost:9003/lifecycle/switch"      # LOCAL node's lifecycle switch endpoint
LIFECYCLE_URL="http://localhost:9003/lifecycle"          # LOCAL node lifecycle status
TARGET_ROLE="primary"                                    # role to switch the local node to
SWITCH_TIMEOUT_MS=5000                                   # timeout_ms in the switch payload
FAIL_THRESHOLD=5                                         # consecutive health failures before failover
CHECK_INTERVAL=2                                         # seconds between health checks
SWITCH_RETRIES=3                                         # retries if the switch POST is not accepted
SWITCH_POLL_INTERVAL=1                                   # seconds between lifecycle polls
SWITCH_POLL_MAX=30                                       # max polls to wait for the switch to settle
# ==================================================

if [[ -z "${QDB_REST_TOKEN:-}" ]]; then
  echo "ERROR: QDB_REST_TOKEN is not set. Aborting."
  exit 2
fi

log() { printf '[%(%Y-%m-%d %H:%M:%S)T] %s\n' -1 "$*"; }

# Ping the primary. Prints the HTTP code, or 000 if the request failed entirely.
get_http_code() {
  local code rc
  code="$(curl -ksS -o /dev/null -w '%{http_code}' \
      -H "Authorization: Bearer $QDB_REST_TOKEN" \
      --connect-timeout 2 --max-time 3 "$PRIMARY_HEALTH_URL" 2>/dev/null)"
  rc=$?
  (( rc != 0 )) && code="000"
  printf '%s' "$code"
}

# Poll /lifecycle until the async switch finishes, then verify the role took.
wait_switch_complete() {
  local want="${TARGET_ROLE^^}"   # lifecycle reports roles uppercase, e.g. PRIMARY
  local attempt resp nospace
  for (( attempt=1; attempt<=SWITCH_POLL_MAX; attempt++ )); do
    resp="$(curl -ksS -H "Authorization: Bearer $QDB_REST_TOKEN" "$LIFECYCLE_URL" 2>/dev/null)"
    nospace="${resp// /}"          # drop spaces so matching is whitespace-agnostic
    if [[ "$nospace" == *'"switchInFlight":false'* ]]; then
      if [[ "$nospace" == *"\"currentRole\":\"$want\""* ]]; then
        log "Switch complete: currentRole=$want"
        return 0
      fi
      log "Switch settled but currentRole != $want. Response: $resp"
      return 1
    fi
    log "switchInFlight=true, waiting for completion ($attempt/$SWITCH_POLL_MAX)..."
    sleep "$SWITCH_POLL_INTERVAL"
  done
  log "ERROR: switch did not settle within $((SWITCH_POLL_MAX * SWITCH_POLL_INTERVAL))s"
  return 1
}

# POST the switch, retry if not accepted, then wait for it to settle.
promote_to_primary() {
  local attempt code body
  for (( attempt=1; attempt<=SWITCH_RETRIES; attempt++ )); do
    log "Requesting switch to $TARGET_ROLE via $SWITCH_URL (attempt $attempt/$SWITCH_RETRIES)"
    body="$(curl -ksS -w $'\n%{http_code}' \
      -X POST "$SWITCH_URL" \
      -H "Authorization: Bearer $QDB_REST_TOKEN" \
      -H "Content-Type: application/json" \
      --data "{\"role\":\"${TARGET_ROLE}\",\"timeout_ms\":${SWITCH_TIMEOUT_MS}}" 2>&1)"
    code="${body##*$'\n'}"   # last line = HTTP code
    body="${body%$'\n'*}"    # everything before it = response body
    log "Switch HTTP $code: $body"
    if [[ "$code" == 2* ]]; then
      wait_switch_complete && return 0   # accepted -> poll until it actually settles
      return 1                            # accepted but did not become $TARGET_ROLE
    fi
    sleep 1                               # non-2xx: retry the POST
  done
  log "ERROR: switch request failed after $SWITCH_RETRIES attempts"
  return 1
}

main() {
  local fail_count=0
  log "Watching primary $PRIMARY_HEALTH_URL (threshold $FAIL_THRESHOLD, every ${CHECK_INTERVAL}s)"
  while true; do
    local code
    code="$(get_http_code)"
    if [[ "$code" == "200" ]]; then
      (( fail_count > 0 )) && log "OK 200, resetting fail counter (was $fail_count)"
      fail_count=0
    else
      (( fail_count++ ))
      log "Health check failed, HTTP $code (consecutive fails: $fail_count/$FAIL_THRESHOLD)"
    fi

    if (( fail_count >= FAIL_THRESHOLD )); then
      log "Primary is down, initiating failover"
      promote_to_primary || log "Failover call failed"
      log "Done, exiting"
      exit 0
    fi

    sleep "$CHECK_INTERVAL"
  done
}

trap 'log "Exiting"; exit 0' INT TERM
main
