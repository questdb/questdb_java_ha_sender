  python csv_columnar_sender.py \
    --addrs localhost:9000 \
    --total-events 10000000 \
    --num-senders 1 \
    --chunk-rows 100000 \
    --csv ../trades20250728.csv.gz
