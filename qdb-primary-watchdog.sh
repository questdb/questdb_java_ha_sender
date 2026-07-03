#!/usr/bin/env bash
# qdb-primary-watchdog.sh
#
# Watches an Enterprise cluster for primary failure and promotes the next node.
#
# Give it the ordered list of lifecycle admin endpoints (host:9003) of ALL nodes. On start it
# finds which one currently reports role PRIMARY and monitors it; if none is primary yet it
# promotes the first healthy node in list order, so a freshly-started cluster gets a primary.
# (Split-brain is not a concern: QuestDB stops the offending node if two ever claim primary.)
# Monitoring is role-based, not
# just a health ping: each interval it reads the node's currentRole, so it reacts both to the
# primary going unreachable AND to it being silently demoted to a replica out-of-band. When the
# primary is lost it first re-checks whether the cluster already has a primary (e.g. an operator
# switch) and adopts it; otherwise it asks the old primary to step down and promotes the first
# HEALTHY node in list order (preferred first, skipping the failed one) via POST /lifecycle/switch,
# waits for the switch to settle, then monitors the new primary. It never exits on its own: if no
# primary exists yet, or none can be promoted, it keeps retrying every check interval.
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

log() { printf '[%(%Y-%m-%d %H:%M:%S)T] %s\n' -1 "$*"; }

# Load KEY=VALUE lines from a config file into the environment, but never override a variable
# already set (so an inline `VAR=x ./script`, or systemd's EnvironmentFile, wins over the file).
# This lets the same script + config file work whether run by hand or under systemd.
load_env_file() {
  local file="$1" line key val
  [[ -r "$file" ]] || return 0
  log "Loading config from $file"
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"; line="${line#"${line%%[![:space:]]*}"}"   # strip CR + left-trim
    [[ -z "$line" || "$line" == '#'* ]] && continue
    [[ "$line" == export\ * ]] && line="${line#export }"
    [[ "$line" != *=* ]] && continue
    key="${line%%=*}"; key="${key%%[[:space:]]*}"
    val="${line#*=}"
    val="${val#"${val%%[![:space:]]*}"}"; val="${val%"${val##*[![:space:]]}"}"   # trim
    if [[ "$val" == \"*\" || "$val" == \'*\' ]]; then val="${val:1:${#val}-2}"; fi
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ && -z "${!key+x}" ]] && export "$key=$val"
  done < "$file"
}

# Config file discovery: explicit $QDB_WD_CONFIG wins, else the first existing standard path.
CONFIG_FILE="${QDB_WD_CONFIG:-}"
if [[ -z "$CONFIG_FILE" ]]; then
  for c in /etc/qdb-primary-watchdog.env \
           "${XDG_CONFIG_HOME:-$HOME/.config}/qdb-primary-watchdog.env" \
           "$(dirname "$(readlink -f "$0" 2>/dev/null || echo "$0")")/qdb-primary-watchdog.env"; do
    [[ -r "$c" ]] && { CONFIG_FILE="$c"; break; }
  done
fi
[[ -n "$CONFIG_FILE" ]] && load_env_file "$CONFIG_FILE"

# ===================== Config =====================
# REQUIRED, no default: ordered lifecycle admin endpoints (host:port, the :9003 port) of every
# node in the cluster. Taken from the first CLI arg, else $QDB_WD_SERVERS.
SERVERS_CSV="${1:-${QDB_WD_SERVERS:-}}"

# Everything below has a default; override via the matching QDB_WD_* environment variable.
TARGET_ROLE="${QDB_WD_TARGET_ROLE:-primary}"              # role to switch a node to on failover
SWITCH_TIMEOUT_MS="${QDB_WD_SWITCH_TIMEOUT_MS:-5000}"     # timeout_ms in the switch payload
STEPDOWN_CONNECT_TIMEOUT="${QDB_WD_STEPDOWN_CONNECT_TIMEOUT:-3}" # connect timeout (s) for the old-primary step-down
CHECK_CONNECT_TIMEOUT="${QDB_WD_CHECK_CONNECT_TIMEOUT:-2}" # connect timeout (s) for each role/health poll
CHECK_MAX_TIME="${QDB_WD_CHECK_MAX_TIME:-3}"             # overall max time (s) for each role/health poll
FAIL_THRESHOLD="${QDB_WD_FAIL_THRESHOLD:-3}"              # consecutive non-primary reads before failover
CHECK_INTERVAL="${QDB_WD_CHECK_INTERVAL:-1}"             # seconds between role checks
GRACE_PERIOD="${QDB_WD_GRACE_PERIOD:-5}"                # seconds to wait after detecting loss before
                                                         # acting, so a manual operator switch can settle
                                                         # (re-checked afterwards; 0 disables the wait)
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

# GET a node's /lifecycle. Prints the raw JSON, or nothing if unreachable.
lifecycle_json() {
  curl -ksS --connect-timeout "$CHECK_CONNECT_TIMEOUT" --max-time "$CHECK_MAX_TIME" \
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
# is ignored. Only the CONNECT phase is bounded (STEPDOWN_CONNECT_TIMEOUT, default 3s) so an
# unreachable host fails fast instead of hanging ~2 min; there is no overall timeout, so once
# connected the step-down runs however long it needs.
demote_to_replica() {
  local hostport="$1" code body
  log "Asking old primary $hostport to step down to replica first (best effort)"
  body="$(curl -ksS -w $'\n%{http_code}' --connect-timeout "$STEPDOWN_CONNECT_TIMEOUT" \
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

# Promote the first HEALTHY node in list order (earliest/preferred wins), skipping the current
# (failed) primary. So with a,b,c where b is primary and dies, it tries a first, then c; it only
# reaches c if a is unhealthy or cannot be promoted. Each candidate's role is read first: an
# unreachable node is skipped, a node that is already PRIMARY is adopted as-is (an out-of-band
# switch beat us to it), and a healthy replica is promoted. Sets FOUND_IDX, returns 0/1.
failover_from() {
  local cur="$1" n="${#SERVERS[@]}" i role
  for (( i=0; i<n; i++ )); do
    (( i == cur )) && continue          # never fail back to the node that just died
    role="$(get_role "${SERVERS[$i]}")"
    if [[ -z "$role" ]]; then
      log "  ${SERVERS[$i]} (index $i) unreachable, skipping"
      continue
    fi
    if [[ "$role" == "PRIMARY" ]]; then
      log "  ${SERVERS[$i]} (index $i) is already PRIMARY, adopting"
      FOUND_IDX=$i
      return 0
    fi
    log "Attempting failover to ${SERVERS[$i]} (index $i, role=$role)"
    if promote "${SERVERS[$i]}"; then
      FOUND_IDX=$i
      return 0
    fi
    log "  ${SERVERS[$i]} could not be promoted, trying next"
  done
  return 1
}

# Ensure the cluster has a primary: adopt an existing one if present, otherwise promote the first
# healthy node in list order (nothing to skip -- pass -1). Used at startup so a cluster that comes
# up with no primary at all gets one. Split-brain is not a concern: QuestDB stops the offending
# node if two ever claim primary. Sets ENSURED_IDX and returns 0, or returns 1 if none could be
# made primary (all nodes unhealthy).
ensure_primary() {
  if find_primary; then
    ENSURED_IDX=$FOUND_IDX
    return 0
  fi
  log "No node is PRIMARY; promoting the first healthy node in list order"
  if failover_from -1; then
    ENSURED_IDX=$FOUND_IDX
    return 0
  fi
  return 1
}

main() {
  log "Cluster (${#SERVERS[@]} nodes): ${SERVERS[*]}"
  log "Detecting current primary..."
  local cur
  # Startup: ensure the cluster has a primary. Adopt an existing one, or promote the first healthy
  # node if none is primary yet. If every node is unhealthy, keep retrying -- never exit.
  until ensure_primary; do
    log "No primary and none could be promoted yet; retrying in ${CHECK_INTERVAL}s (will not exit)..."
    sleep "$CHECK_INTERVAL"
  done
  cur=$ENSURED_IDX
  log "Primary is ${SERVERS[$cur]} (index $cur). Monitoring every ${CHECK_INTERVAL}s (threshold $FAIL_THRESHOLD)."

  local fail_count=0 role
  while true; do
    # Role-based check: the primary counts as healthy only while it still reports PRIMARY.
    # An unreachable node (empty role) or a silent demotion to REPLICA both count as failures.
    role="$(get_role "${SERVERS[$cur]}")"
    if [[ "$role" == "PRIMARY" ]]; then
      (( fail_count > 0 )) && log "Primary ${SERVERS[$cur]} healthy again (role=PRIMARY), resetting"
      fail_count=0
    else
      (( fail_count++ ))
      log "Primary ${SERVERS[$cur]} not serving as primary (role=${role:-unreachable}) (fails: $fail_count/$FAIL_THRESHOLD)"
    fi

    if (( fail_count >= FAIL_THRESHOLD )); then
      log "Primary ${SERVERS[$cur]} lost; initiating failover"
      # Grace period: give an operator doing a manual switch (demote the old primary, promote a
      # replica) a few seconds to finish before we act, so the watchdog does not race them. After
      # the wait we re-check the cluster; if a primary is now present we simply adopt it below.
      if (( GRACE_PERIOD > 0 )); then
        log "  grace period: waiting ${GRACE_PERIOD}s for a manual switch to settle before acting"
        sleep "$GRACE_PERIOD"
      fi
      # The cluster may already have a new primary (operator switch, or cur recovered elsewhere in
      # the list): adopt it instead of forcing another switch.
      if find_primary; then
        cur=$FOUND_IDX
        fail_count=0
        log "Cluster already has a PRIMARY: ${SERVERS[$cur]} (index $cur). Monitoring it."
      else
        demote_to_replica "${SERVERS[$cur]}"   # best-effort: make the old primary stand down first
        if failover_from "$cur"; then
          cur=$FOUND_IDX
          fail_count=0
          log "New primary is ${SERVERS[$cur]} (index $cur). Monitoring."
        else
          # No healthy node to promote. Do NOT exit: leave fail_count armed so the next tick
          # retries the whole failover, and keep going until some node can take over.
          log "No healthy node could be promoted; will keep retrying every ${CHECK_INTERVAL}s (not exiting)"
        fi
      fi
    fi

    sleep "$CHECK_INTERVAL"
  done
}

trap 'log "Exiting"; exit 0' INT TERM
main
