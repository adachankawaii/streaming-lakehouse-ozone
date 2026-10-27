-- Smoke test: real Iceberg metadata + Parquet on Apache Ozone

SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';

USE CATALOG lakehouse;
USE bronze;

DROP TABLE IF EXISTS flink_ozone_smoke;

CREATE TABLE flink_ozone_smoke (
  id BIGINT,
  message STRING,
  event_time TIMESTAMP(3)
);

INSERT INTO flink_ozone_smoke
VALUES (
  1,
  'Flink -> Lakekeeper -> Iceberg -> Ozone',
  TIMESTAMP '2026-08-27 17:00:00.000'
);

SELECT * FROM flink_ozone_smoke;
