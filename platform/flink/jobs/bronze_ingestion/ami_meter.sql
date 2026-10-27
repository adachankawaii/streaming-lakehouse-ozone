-- ============================================================
-- Job 1: AMI / Smart Meter -> Kafka -> Flink -> Iceberg Bronze
-- Target: lakehouse.bronze.ami_meter_raw
-- ============================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';

USE CATALOG lakehouse;
USE bronze;

-- Kafka source intentionally reads key/value as raw UTF-8 strings.
-- This keeps malformed or future-schema payloads available for Bronze/audit.
CREATE TEMPORARY TABLE kafka_ami_meter_events (
  kafka_key STRING,
  raw_payload STRING,

  kafka_topic STRING METADATA FROM 'topic' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'ami.meter.events',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'bronze-ami-meter-v1',
  'properties.enable.auto.commit' = 'false',
  'properties.auto.offset.reset' = 'earliest',

  'scan.startup.mode' = 'group-offsets',

  'key.format' = 'raw',
  'key.fields' = 'kafka_key',

  'value.format' = 'raw',
  'value.fields-include' = 'EXCEPT_KEY'
);

-- Bronze keeps source payload + Kafka provenance.
-- Only a small set of source fields is extracted here for inspection/routing.
-- Canonical IDs, normalized time/units, dedup and business-quality logic belong to Silver.
CREATE TABLE IF NOT EXISTS ami_meter_raw (
  source_system STRING,
  source_format STRING,
  ingest_time TIMESTAMP_LTZ(3),

  kafka_topic STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP_LTZ(3),
  kafka_key STRING,

  schema_version STRING,
  event_id STRING,
  event_type STRING,
  meter_id STRING,
  usage_point_id STRING,
  event_time_source STRING,
  sequence_no BIGINT,

  raw_payload STRING,
  raw_payload_hash STRING,
  parse_status STRING,

  bronze_date DATE
)
PARTITIONED BY (bronze_date)
WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'write.target-file-size-bytes' = '134217728'
);

INSERT INTO ami_meter_raw
SELECT
  'AMI' AS source_system,
  'json' AS source_format,
  CURRENT_TIMESTAMP AS ingest_time,

  kafka_topic,
  kafka_partition,
  kafka_offset,
  kafka_timestamp,
  kafka_key,

  JSON_VALUE(raw_payload, 'lax $.schema_version') AS schema_version,
  JSON_VALUE(raw_payload, 'lax $.event_id') AS event_id,
  JSON_VALUE(raw_payload, 'lax $.event_type') AS event_type,
  JSON_VALUE(raw_payload, 'lax $.meter_id') AS meter_id,
  JSON_VALUE(raw_payload, 'lax $.usage_point_id') AS usage_point_id,
  JSON_VALUE(raw_payload, 'lax $.event_time') AS event_time_source,
  TRY_CAST(JSON_VALUE(raw_payload, 'lax $.sequence_no') AS BIGINT) AS sequence_no,

  raw_payload,
  SHA256(raw_payload) AS raw_payload_hash,

  CASE
    WHEN NOT (raw_payload IS JSON OBJECT)
      THEN 'MALFORMED'

    WHEN JSON_VALUE(raw_payload, 'lax $.schema_version') IS NULL
      OR JSON_VALUE(raw_payload, 'lax $.event_id') IS NULL
      OR JSON_VALUE(raw_payload, 'lax $.event_type') IS NULL
      OR JSON_VALUE(raw_payload, 'lax $.meter_id') IS NULL
      OR JSON_VALUE(raw_payload, 'lax $.usage_point_id') IS NULL
      OR JSON_VALUE(raw_payload, 'lax $.event_time') IS NULL
      OR TRY_CAST(JSON_VALUE(raw_payload, 'lax $.sequence_no') AS BIGINT) IS NULL
      THEN 'PARTIAL'

    WHEN JSON_VALUE(raw_payload, 'lax $.schema_version') <> '1.0'
      THEN 'UNSUPPORTED_SCHEMA'

    ELSE 'OK'
  END AS parse_status,

  CURRENT_DATE AS bronze_date
FROM kafka_ami_meter_events;
