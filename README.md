# Sample Sender with Multiple Hosts for HA Ingestion

## Compile

`mvn  -DskipTests clean package`

## Usage Example

```
java -jar target/ilp_sender-1.0-SNAPSHOT.jar \
--addrs enterprise-primary:9000,enterprise-replica:9000,enterprise-replica2:9000 \
--token "$ILP_TOKEN" \
--total-events 50000000 \
--delay-ms 0 \
--num-senders 8 \
--csv ./trades20250728.csv.gz \
--timestamp-from-file false \
--retry-timeout 360000
```


For a similar sender, but for telemetry data, see the instructions (and license info) at [the Telemetry Readme](./TelemetryReadme.md)
