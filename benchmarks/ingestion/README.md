# AMI RAMP benchmark collector

This benchmark is designed for the continuous-ingestion experiment:

`AMI simulator -> Kafka -> Flink -> Iceberg -> Apache Ozone`

Default workload:

- 60 s at 1,000 events/s
- 60 s at 2,000 events/s
- 60 s at 5,000 events/s
- 30 s drain/observation window

Run:

```powershell
.\scripts\run-ami-ramp-benchmark.ps1
```

Outputs are written to:

`benchmarks/results/<timestamp>-ami-ramp/`

Files:

- `metrics.csv`: periodic time-series metrics.
- `checkpoints.csv`: one row per newly observed completed Flink checkpoint.
- `iceberg_commits.csv`: one row whenever committed-record count changes.
- `stage_summary.csv`: per-stage summary for the report.
- `benchmark_manifest.json`: run configuration and resolved metric names.
- `simulator.log`: generator output.

## Metric semantics

`kafka_input_rate_eps`
: Broker-side append rate, computed from total Kafka `LOG-END-OFFSET` delta / wall-clock time.

`kafka_committed_consume_rate_eps`
: Rate computed from consumer-group committed offset delta. Since Flink commits Kafka offsets with completed checkpoints, this moves in steps and should not be interpreted as instantaneous operator throughput.

`flink_source_rate_eps`
: Delta of Flink source `numRecordsOut` / wall-clock time.

`iceberg_last_commit_duration_ms`
: Native Iceberg Flink sink metric for table commit duration.

`iceberg_seconds_since_last_commit`
: Native Iceberg metric useful for spotting commit stalls.

`ozone_logical_bytes`
: Sum of logical file sizes returned by `ozone fs -ls -R`. It is not RATIS physical replicated storage usage.

Ozone inventory is sampled less frequently than Kafka/Flink metrics to reduce observer overhead during the benchmark.

## Latency

Accelerated AMI replay separates business event-time from wall-clock emission time.

For pipeline ingestion latency, use:

`Bronze ingest_time - kafka_timestamp`

Do not use:

`ingest_time - event_time_source`

because `event_time_source` represents replayed business history.
