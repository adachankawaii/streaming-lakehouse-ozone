# Hai đường logic trong Lakehouse

```text
Write/data: AMI -> Kafka -> Flink -> Iceberg writer -> Ozone S3G -> Ozone DataNode
                              |
Catalog/commit:               +---> Lakekeeper REST -> PostgreSQL

Read/metadata: Trino -> Lakekeeper -> snapshot/manifest
Read/files:    Trino -> Ozone S3G -> Parquet
```

Không mô tả Iceberg hoặc Lakekeeper là nơi lưu byte Parquet. Cần tách latency commit của Iceberg/catalog khỏi thời gian PUT/GET S3 Gateway và IO đến Ozone. Một benchmark pipeline bị chậm không đủ chứng minh OM/SCM là bottleneck.

Local Compose: 1 OM + 1 SCM + 5 DataNode; RATIS/THREE. HA control-plane không được kiểm thử trong cấu hình này. Lab VDP/Ambari là môi trường **khác** (Ozone 2.2.1, 3 DN) và phải có source riêng.
