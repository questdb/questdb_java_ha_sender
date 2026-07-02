#!/usr/bin/env bash
# qdb-primary-watchdog.sh
#
# Watches an Enterprise cluster for primary failure and promotes the next node.
#
# Give it the ordered list of lifecycle admin endpoints (host:9003) of ALL nodes. On start it
# finds which one currently reports role PRIMARY and health-monitors it. When that primary
# stops responding, it promotes the first reachable node in list order (earliest first,
# skipping the failed one) via POST /lifecycle/switch, waits for the switch to settle, then
# monitors the newly promoted node. If no node can be promoted (the whole cluster is down),
# it exits.
#
# Required, no defaults: the node list and QDB_REST_TOKEN. The node list comes from the first
# CLI arg or $QDB_WD_SERVERS; the token from $QDB_REST_TOKEN. Everything else has a default
# and can be overridden via its QDB_WD_* variable. All endpoints are https (Enterprise only).
#
# Usage:
#   export QDB_REST_TOKEN="..."
#   ./qdb-primary-watchdog.sh h1:9003,h2:9003,h3:9003                 # node list as arg, or:
#   QDB_WD_SERVERS=h1:9003,h2:9003,h3:9003 ./qdb-primary-watchdog.sh

# Do NOT source this script
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  echo "Please RUN this script, do not source it. Try: bash ${BASH_SOURCE[0]}"
  return 1 2>/dev/null || exit 1
fi

set -o pipefail
LC_ALL=C

# ===================== Config =====================
# REQUIRED, no default: ordered lifecycle admin endpoints (host:port, the :9003 port) of every
# node in the cluster. Taken from the first CLI arg, else $QDB_WD_SERVERS.
SERVERS_CSV="${1:-${QDB_WD_SERVERS:-}}"

# Everything below has a default; override via the matching QDB_WD_* environment variable.
TARGET_ROLE="${QDB_WD_TARGET_ROLE:-primary}"              # role to switch a node to on failover
SWITCH_TIMEOUT_MS="${QDB_WD_SWITCH_TIMEOUT_MS:-5000}"     # timeout_ms in the switch payload
FAIL_THRESHOLD="${QDB_WD_FAIL_THRESHOLD:-3}"              # consecutive health failures before failover
CHECK_INTERVAL="${QDB_WD_CHECK_INTERVAL:-1}"             # seconds between health checks
STARTUP_RETRIES="${QDB_WD_STARTUP_RETRIES:-3}"          # sweeps to find the initial primary before giving up
SWITCH_RETRIES="${QDB_WD_SWITCH_RETRIES:-3}"            # retries if a switch POST is not accepted
SWITCH_POLL_INTERVAL="${QDB_WD_SWITCH_POLL_INTERVAL:-1}" # seconds between lifecycle polls after a switch
SWITCH_POLL_MAX="${QDB_WD_SWITCH_POLL_MAX:-30}"         # max polls to wait for a switch to settle
# QDB_REST_TOKEN is also REQUIRED (no default); checked below.
# ==================================================

if [[ -z "${QDB_REST_TOKEN:-}" ]]; then
  echo "ERROR: QDB_REST_TOKEN is required (no default). Set it in the environment." >&2
  exit 2
fi

if [[ -z "$SERVERS_CSV" ]]; then
  echo "ERROR: no node list (no default). Pass it as the first argument or set QDB_WD_SERVERS." >&2
  echo "  e.g. $0 h1:9003,h2:9003,h3:9003" >&2
  exit 2
fi

IFS=',' read -r -a SERVERS <<< "$SERVERS_CSV"
if (( ${#SERVERS[@]} == 0 )); then
  echo "ERROR: node list is empty." >&2
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

# Best-effort: tell the (presumed-down) old primary to step down to replica before we promote a
# new one. Guards against a false positive where the primary is actually still up, so we never
# end up with two primaries. Expected to usually fail (the primary really is down); the result
# is ignored. Intentionally NO timeout on this curl: if the old primary is reachable, let the
# step-down complete however long it needs.
demote_to_replica() {
  local hostport="$1" code body
  log "Asking old primary $hostport to step down to replica first (best effort)"
  body="$(curl -ksS -w $'\n%{http_code}' \
      -X POST "https://$hostport/lifecycle/switch" \
      -H "Authorization: Bearer $QDB_REST_TOKEN" \
      -H "Content-Type: application/json" \
      --data '{"role":"replica"}' 2>&1)"
  code="${body##*$'\n'}"
  body="${body%$'\n'*}"
  log "  step-down $hostport HTTP $code: $body"
}

# Promote one node to primary. On a failed attempt it retries with exponential backoff, using
# SWITCH_TIMEOUT_MS as the base delay: ~5s before the 2nd attempt, ~10s before the 3rd, doubling.
# Returns 0 only if a switch is accepted and the node settles as primary.
promote() {
  local hostport="$1" attempt code body delay_ms="$SWITCH_TIMEOUT_MS"
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
    if [[ "$code" == 2* ]] && wait_switch_complete "$hostport"; then
      return 0
    fi
    if (( attempt < SWITCH_RETRIES )); then
      log "  promote attempt $attempt did not take, retrying in $((delay_ms / 1000))s"
      sleep "$(( delay_ms / 1000 ))"
      delay_ms=$(( delay_ms * 2 ))
    fi
  done
  log "  could not promote $hostport after $SWITCH_RETRIES attempts"
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

# Promote the first reachable node in list order (earliest wins), skipping the current
# (failed) primary. So with a,b,c where b is primary and dies, it tries a first, then c;
# it only reaches c if a cannot be promoted. Sets FOUND_IDX, returns 0/1.
failover_from() {
  local cur="$1" n="${#SERVERS[@]}" i
  for (( i=0; i<n; i++ )); do
    (( i == cur )) && continue          # never fail back to the node that just died
    log "Attempting failover to ${SERVERS[$i]} (index $i)"
    if promote "${SERVERS[$i]}"; then
      FOUND_IDX=$i
      return 0
    fi
    log "  ${SERVERS[$i]} could not be promoted, trying next"
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
      demote_to_replica "${SERVERS[$cur]}"   # first, try to make the old primary stand down
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
