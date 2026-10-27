# AMI Simulator

Modes:

- `baseline`: 3,000 meters / 900 s ≈ 3.33 events/s, acceleration 1x.
- `ramp`: 1,000 -> 2,000 -> 5,000 events/s.
- `burst`: 1,000 events/s warm-up -> 10,000 events/s burst -> 1,000 events/s recovery.
- `quality`: 100 events/s with malformed, partial, unsupported-schema, duplicate, late and sequence-gap injection.
- `custom`: caller supplies `--target-rate`.

The payload always retains the 900-second business interval.
Benchmark rate changes wall-clock replay/acceleration, not the meaning of the meter sampling interval.

# AMI Simulator clock semantics

The simulator separates:

- `event_time`: logical/business meter-reading time.
- Kafka record timestamp: wall-clock emission time.
- Bronze `ingest_time`: Flink ingestion time.

For accelerated replay, measure pipeline latency with:

`ingest_time - kafka_timestamp`

Do not use:

`ingest_time - event_time_source`

because accelerated replay intentionally replays historical business time faster than wall clock.

For finite accelerated workloads, the simulator automatically backdates `event_time` far enough so the final logical timestamp does not move into the future.

Examples:
- 3,000 meters, 900 s interval, 100 events/s for 30 s = 3,000 records = one logical cycle = about 15 minutes of history.
- Ramp 1K -> 2K -> 5K for 60 s each = about 480,000 records = 160 logical cycles = about 40 hours of history.
