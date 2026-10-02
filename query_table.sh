#!/usr/bin/env bash
# Read the last N rows of a table with the Java QWP reader (com.example.reader.ReadBench).
#
# With no arguments it runs the demo read against $QDB_ADDR (default: all three cluster
# nodes, so failover is exercised),
# authenticating with $ILP_TOKEN. With arguments, they are passed straight to ReadBench:
#
#   ./query_table.sh trades --addr h1:9000,h2:9000 --limit 200000000 \
#       --token "$ILP_TOKEN" --tls-verify unsafe_off --readers 8 --sample 5
#
# --addr takes a comma-separated host list for failover. Run with --help for all options.
set -euo pipefail

cd "$(dirname "$0")"
JAR=target/ilp_sender-1.0-SNAPSHOT.jar

if [ ! -f "$JAR" ] || [ -n "$(find src -newer "$JAR" -name '*.java' -print -quit)" ]; then
    mvn -q -DskipTests package
fi

if [ "$#" -eq 0 ]; then
    : "${ILP_TOKEN:?set ILP_TOKEN to your QuestDB token}"
    set -- trades \
        --addr "${QDB_ADDR:-172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000}" \
        --limit 200000000 \
        --token "$ILP_TOKEN" \
        --tls-verify unsafe_off \
        --readers 8 \
        --sample 5
fi

exec java -cp "$JAR" com.example.reader.ReadBench "$@"
