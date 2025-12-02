Create the telemetry table with

```sql
CREATE TABLE cisco_baseline_500gbps (
    name SYMBOL,
    time TIMESTAMP,
    'EncodingPath' SYMBOL,
    'Producer' SYMBOL,
    'acl_in_rpf_packets' LONG,
    'active_routes_count' LONG,
    'af_name' SYMBOL,
    'as' INT,
    'backup_routes_count' LONG,
    bandwidth LONG,
    'bytes_received' LONG,
    'bytes_sent' LONG,
    'carrier_transitions' LONG,
    'checksum_error_packets' LONG,
    'crc_errors' LONG,
    'deleted_routes_count' LONG,
    'df_unreachable_packets' LONG,
    'discard_packets' LONG,
    'encapsulation_failure_packets' LONG,
    'fragmenation_consumed_packets' LONG,
    'fragmenation_failure_packets' LONG,
    'free_application_memory' LONG,
    'free_physical_memory' LONG,
    'global__established_neighbors_count_total' LONG,
    'global__neighbors_count_total' LONG,
    'global__nexthop_count' LONG,
    'global__restart_count' LONG,
    'gre_error_drop' LONG,
    'gre_lookup_failed_drop' LONG,
    'incomplete_adjacency_packets' LONG,
    'input_data_rate' LONG,
    'input_drops' LONG,
    'input_errors' LONG,
    'input_ignored_packets' LONG,
    'input_load' LONG,
    'input_packet_rate' LONG,
    'input_queue_drops' LONG,
    'instance_name' SYMBOL,
    'interface_name' SYMBOL,
    'lisp_decap_error_drops' LONG,
    'lisp_encap_error_drops' LONG,
    'lisp_punt_drops' LONG,
    'load_interval' LONG,
    'mpls_disabled_interface' LONG,
    'multi_label_drops' LONG,
    'no_route_packets' LONG,
    'node_name' SYMBOL,
    'null_packets' LONG,
    'output_buffer_failures' LONG,
    'output_data_rate' LONG,
    'output_drops' LONG,
    'output_errors' LONG,
    'output_load' LONG,
    'output_packet_rate' LONG,
    'output_queue_drops' LONG,
    'packets_received' LONG,
    'packets_sent' LONG,
    'paths_count' LONG,
    'peak_input_data_rate' LONG,
    'peak_input_packet_rate' LONG,
    'peak_output_data_rate' LONG,
    'peak_output_packet_rate' LONG,
    'performance_statistics__global__configuration_items_processed' LONG,
    'performance_statistics__global__ipv4rib_server__is_rib_connection_up' BOOLEAN,
    'performance_statistics__global__ipv4rib_server__rib_connection_up_count' LONG,
    'performance_statistics__vrf__inbound_update_messages' LONG,
    'protocol_route_memory' LONG,
    'punt_unreachable_packets' LONG,
    'ram_memory' LONG,
    reliability LONG,
    'route_table_name' SYMBOL,
    'routes_counts' LONG,
    'rp_destination_drop_packets' LONG,
    'rpf_check_failure_packets' LONG,
    'saf_name' SYMBOL,
    'system_ram_memory' LONG,
    'total_cpu_fifteen_minute' LONG,
    'total_cpu_five_minute' LONG,
    'total_cpu_one_minute' LONG,
    'total_number_of_drop_packets' LONG,
    'unresolved_prefix_packets' LONG,
    'unsupported_feature_packets' LONG,
    'vrf_name' SYMBOL,
    'vrf__neighbors_count' LONG,
    'vrf__network_count' LONG,
    'vrf__path_count' LONG,
    'vrf__update_messages_received' LONG
) TIMESTAMP(time) PARTITION BY DAY;
```

Compile

```bash
mvn -DskipTests package
```

Send data with

```bash
mvn exec:java \                                                                                                                                                                                                                                                                 🙈
  -Dexec.mainClass=com.example.sender.TelemetryParallelSender \
  -Dexec.args="--addrs localhost:9000 \
               --csv ./cisco_baseline_500gbps.csv.gz \
               --total-events 1000 \
               --num-senders 1 \
               --delay-ms 0"
```

Data originally from https://github.com/javier/cisco-ie-telemetry/tree/master
