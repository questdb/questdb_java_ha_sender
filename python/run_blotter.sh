#!/usr/bin/env bash
# Live blotter. Set your token first (export ILP_TOKEN=<token>), then: ./run_blotter.sh
# Edit the params below directly.

python blotter.py \
    --table core_price_lv \
    --where "symbol = 'EURUSD'" \
    --limit -10 \
    --rate 5 \
    --addr 172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000 \
    --token "$ILP_TOKEN" \
    --tls-verify unsafe_off

# --- or run a full custom query instead (ignores --table/--where/--limit) ---
# python blotter.py \
#     --query "with x as (select * from core_price_lv where symbol = 'EURUSD' order by timestamp desc limit 50) select *, avg(bid_price) over (partition by symbol) as avg50 from x" \
#     --rate 5 \
#     --addr 172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000 \
#     --token "$ILP_TOKEN" \
#     --tls-verify unsafe_off
