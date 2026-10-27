# Concurrent read/write — planned

Không có runner *đồng thời* hoàn chỉnh trong `ozone-demo.zip` gốc. Có thể dùng hai terminal để chạy AMI ingestion và Trino fixed-snapshot read như một pilot, nhưng phải lưu manifest/bắt đầu-kết thúc theo cùng cửa sổ; không gọi kết quả pilot là benchmark được kiểm soát.

- Baseline: Trino queries trên snapshot cố định, không có input mới (hoặc idle sau drain).
- Concurrent: Trino queries **cùng snapshot/cùng SQL** trong lúc Flink Iceberg writer đang nhận target EPS cố định.
- Đo query p50/p95/p99, QPS, failures; Kafka/Flink achieved throughput, checkpoint, commit và tài nguyên các service.
- Giữ topology và data footprint bất biến khi có thể; kiểm soát warmup/cache/noisy neighbor. Không kết luận Ozone là nguồn gây chậm khi thiếu CPU/GC/catalog/S3G metrics.
