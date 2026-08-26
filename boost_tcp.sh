#!/usr/bin/env bash
# boost_tcp.sh - lift the socket-buffer clamp for QuestDB QWP ingestion.
#
# The Rust/C/Python QWP clients hardcode a 4 MiB socket buffer, which default
# Linux silently clamps to ~416 KB (net.core.wmem_max) AND disables autotuning,
# pinning each connection at ~426 KB per RTT. On a real network this caps
# throughput hard (e.g. ~210k rows/s at 20 ms RTT). Raising the max buffers lets
# the client's 4 MiB request through. Apply on BOTH the sender and server boxes.
#
# SAFE any way you run it: `sudo bash boost_tcp.sh`, `./boost_tcp.sh`,
# `. boost_tcp.sh`, `source boost_tcp.sh`. No set -e, no exec, no exit -> it can
# never kill or exit your shell. Runtime-only: values reset on reboot, so run it
# once per boot (or persist via /etc/sysctl.d/ - see README).
boost_tcp() {
  local S=""
  [ "$(id -u)" -ne 0 ] && S="sudo"
  $S sysctl -w net.core.rmem_max=8388608
  $S sysctl -w net.core.wmem_max=8388608
  $S sysctl -w net.ipv4.tcp_wmem="4096 65536 16777216"
  $S sysctl -w net.ipv4.tcp_rmem="4096 65536 16777216"
  $S sysctl -w net.core.somaxconn=32768
  $S sysctl -w net.core.netdev_max_backlog=65536
  echo "boost_tcp: applied (holds until reboot)"
  $S sysctl net.core.wmem_max net.core.rmem_max
}
boost_tcp
