# Kafka — local Docker

Broker Kafka 3.9.1 KRaft single broker trong [`../../compose/kafka.yml`](../../compose/kafka.yml). Kafka AMI topic `ami.meter.events` được bootstrap ở `scripts/start-safe.ps1`; Flink consumer group `bronze-ami-meter-v1` khai báo trong `platform/flink/jobs/bronze_ingestion/ami_meter.sql`. Đây không phải Kafka 2 broker Kerberos của VDP.
