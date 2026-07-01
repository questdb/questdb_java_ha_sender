#!/usr/bin/env bash
# qdb-primary-watchdog.sh
#
# Watches an Enterprise cluster for primary failure and promotes the next node.
#
# Give it the ordered list of lifecycle admin endpoints (host:9003) of ALL nodes. On start it
# finds which one currently reports role PRIMARY and health-monitors it. When that primary
# stops responding, it promotes the next node in the list (round-robin) via
# POST /lifecycle/switch, waits for the switch to settle, then monitors the new primary. If a
# full pass around the ring finds no node it can promote (the whole cluster is down), it exits.
#
# All endpoints are https (Enterprise only). Requires QDB_REST_TOKEN (bearer token).
#
# Usage:
#   export QDB_REST_TOKEN="..."
#   ./qdb-primary-watchdog.sh                                  # uses DEFAULT_SERVERS below
#   ./qdb-primary-watchdog.sh h1:9003,h2:9003,h3:9003          # or pass the list explicitly

# Do NOT source this script
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  echo "Please RUN this script, do not source it. Try: bash ${BASH_SOURCE[0]}"
  return 1 2>/dev/null || exit 1
fi

set -o pipefail
LC_ALL=C

# ===================== Config =====================
# Ordered lifecycle admin endpoints (host:port, the :9003 port) of every node in the cluster.
DEFAULT_SERVERS="172.31.42.41:9003,172.31.41.35:9003,10.0.0.8:9003"
SERVERS_CSV="${1:-$DEFAULT_SERVERS}"          # first CLI arg overrides the default list

TARGET_ROLE="primary"        # role to switch a node to on failover
SWITCH_TIMEOUT_MS=5000       # timeout_ms in the switch payload
FAIL_THRESHOLD=5             # consecutive health failures before failover
CHECK_INTERVAL=2             # seconds between health checks
STARTUP_RETRIES=3            # sweeps to find the initial primary before giving up
SWITCH_RETRIES=3             # retries if a switch POST is not accepted
SWITCH_POLL_INTERVAL=1       # seconds between lifecycle polls after a switch
SWITCH_POLL_MAX=30           # max polls to wait for a switch to settle
# ==================================================

if [[ -z "${QDB_REST_TOKEN:-}" ]]; then
  echo "ERROR: QDB_REST_TOKEN is not set. Aborting."
  exit 2
fi

IFS=',' read -r -a SERVERS <<< "$SERVERS_CSV"
if (( ${#SERVERS[@]} == 0 )); then
  echo "ERROR: no servers configured."
  exit 2
fi

log() { printf '[%(%Y-%m-%d %H:%M:%S)T] %s\n' -1 "$*"; }

# GET a node's /lifecycle. Prints the raw JSON, or nothing if unreachable.
lifecycle_json() {
  curl -ksS --connect-timeout 2 --max-time 3 \
      -H "Authorization: Bearer $QDB_REST_TOKEN" \
      "https://$1/lifecycle" 2>/dev/null
}

# Print a node's currentRole (uppercase), or empty if unreachable / field absent.
get_role() {
  local resp nospace
  resp="$(lifecycle_json "$1")"
  nospace="${resp// /}"
  if [[ "$nospace" == *'"currentRole":"'* ]]; then
    nospace="${nospace#*'"currentRole":"'}"
    printf '%s' "${nospace%%'"'*}"
  fi
}

# Health ping. Prints the HTTP code, or 000 if there was no response at all.
get_http_code() {
  local code rc
  code="$(curl -ksS -o /dev/null -w '%{http_code}' \
      -H "Authorization: Bearer $QDB_REST_TOKEN" \
      --connect-timeout 2 --max-time 3 "https://$1/lifecycle" 2>/dev/null)"
  rc=$?
  (( rc != 0 )) && code="000"
  printf '%s' "$code"
}

# Poll a node's /lifecycle until its switch settles and it reports the target role.
wait_switch_complete() {
  local hostport="$1" want="${TARGET_ROLE^^}" attempt resp nospace
  for (( attempt=1; attempt<=SWITCH_POLL_MAX; attempt++ )); do
    resp="$(lifecycle_json "$hostport")"
    nospace="${resp// /}"
    if [[ "$nospace" == *'"switchInFlight":false'* ]]; then
      if [[ "$nospace" == *"\"currentRole\":\"$want\""* ]]; then
        log "  switch complete on $hostport: currentRole=$want"
        return 0
      fi
      log "  $hostport settled but currentRole != $want"
      return 1
    fi
    log "  $hostport switchInFlight=true, waiting ($attempt/$SWITCH_POLL_MAX)..."
    sleep "$SWITCH_POLL_INTERVAL"
  done
  log "  $hostport switch did not settle within $((SWITCH_POLL_MAX * SWITCH_POLL_INTERVAL))s"
  return 1
}

# Promote one node to primary. Returns 0 only if it accepts the switch and settles as primary.
promote() {
  local hostport="$1" attempt code body
  for (( attempt=1; attempt<=SWITCH_RETRIES; attempt++ )); do
    log "  requesting switch on $hostport (attempt $attempt/$SWITCH_RETRIES)"
    body="$(curl -ksS -w $'\n%{http_code}' \
        -X POST "https://$hostport/lifecycle/switch" \
        -H "Authorization: Bearer $QDB_REST_TOKEN" \
        -H "Content-Type: application/json" \
        --data "{\"role\":\"${TARGET_ROLE}\",\"timeout_ms\":${SWITCH_TIMEOUT_MS}}" 2>&1)"
    code="${body##*$'\n'}"   # last line = HTTP code
    body="${body%$'\n'*}"    # everything before it = response body
    log "  switch $hostport HTTP $code: $body"
    if [[ "$code" == 2* ]]; then
      wait_switch_complete "$hostport" && return 0
      return 1
    fi
    sleep 1
  done
  return 1
}

# Find the node currently reporting PRIMARY. Sets FOUND_IDX and returns 0, else returns 1.
find_primary() {
  local i role
  for (( i=0; i<${#SERVERS[@]}; i++ )); do
    role="$(get_role "${SERVERS[$i]}")"
    log "  ${SERVERS[$i]} -> role=${role:-unreachable}"
    if [[ "$role" == "PRIMARY" ]]; then
      FOUND_IDX=$i
      return 0
    fi
  done
  return 1
}

# Promote the next reachable node after index $1, round-robin. Sets FOUND_IDX, returns 0/1.
failover_from() {
  local cur="$1" n="${#SERVERS[@]}" step idx
  for (( step=1; step<n; step++ )); do
    idx=$(( (cur + step) % n ))
    log "Attempting failover to ${SERVERS[$idx]} (index $idx)"
    if promote "${SERVERS[$idx]}"; then
      FOUND_IDX=$idx
      return 0
    fi
    log "  ${SERVERS[$idx]} could not be promoted, trying next"
  done
  return 1
}

main() {
  log "Cluster (${#SERVERS[@]} nodes): ${SERVERS[*]}"
  log "Detecting current primary..."
  local cur attempt=1
  until find_primary; do
    if (( attempt >= STARTUP_RETRIES )); then
      log "ERROR: no PRIMARY found among the nodes after $STARTUP_RETRIES sweeps. Exiting."
      exit 1
    fi
    (( attempt++ ))
    log "No primary yet, retrying sweep ($attempt/$STARTUP_RETRIES)..."
    sleep "$CHECK_INTERVAL"
  done
  cur=$FOUND_IDX
  log "Primary is ${SERVERS[$cur]} (index $cur). Monitoring every ${CHECK_INTERVAL}s (threshold $FAIL_THRESHOLD)."

  local fail_count=0 code
  while true; do
    code="$(get_http_code "${SERVERS[$cur]}")"
    # Any HTTP reply means the primary is alive; only a no-response (000) counts as down.
    if [[ "$code" == "000" ]]; then
      (( fail_count++ ))
      log "Primary ${SERVERS[$cur]} unreachable, no HTTP response (fails: $fail_count/$FAIL_THRESHOLD)"
    else
      (( fail_count > 0 )) && log "Primary ${SERVERS[$cur]} responding again (HTTP $code), resetting"
      fail_count=0
    fi

    if (( fail_count >= FAIL_THRESHOLD )); then
      log "Primary ${SERVERS[$cur]} is down, initiating failover"
      if failover_from "$cur"; then
        cur=$FOUND_IDX
        fail_count=0
        log "New primary is ${SERVERS[$cur]} (index $cur). Monitoring."
      else
        log "ERROR: no node could be promoted (whole cluster appears down). Exiting."
        exit 1
      fi
    fi

    sleep "$CHECK_INTERVAL"
  done
}

trap 'log "Exiting"; exit 0' INT TERM
main
