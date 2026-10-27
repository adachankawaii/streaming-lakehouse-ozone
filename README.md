# Streaming Lakehouse trên Apache Ozone

**Source repository (Git-ready) — trạng thái tổng hợp 27/10/2026**

Đây là bộ source **có thể kiểm tra và phát triển tiếp**, được tổ chức từ archive `ozone-demo.zip` của project Ada. Phần code chạy được theo thiết kế gốc là **local Docker Compose lab**; repo **không tự nhận** đã đóng gói toàn bộ runtime hoặc script triển khai thật trên VDP/Ambari.

## Mục tiêu

```text
AMI simulator ──> Kafka ──> Flink SQL ──> Iceberg Bronze ──> Ozone S3G ──> Ozone
                                          │
                                          └── Lakekeeper (REST Catalog + PostgreSQL)

Trino ──> Lakekeeper/Iceberg metadata ──> Parquet/metadata trên Ozone
```

- **Ozone**: storage cho Parquet, metadata Iceberg, checkpoint Flink; **không** phải DB hoặc engine phát hiện bất thường.
- **Flink/Iceberg/Lakekeeper/Trino**: tầng ingest, table semantics, catalog và query.
- **Benchmark**: kiểm tra behavior của **toàn đường đi** và ghi đủ bằng chứng, không suy từ benchmark này rằng Ozone đơn lẻ là bottleneck hay luôn nhanh hơn HDFS/MinIO.

## Cấu trúc thư mục

```text
.
├── compose.yaml                     # Entrypoint Docker Compose
├── compose/                        # Service topology, JVM, resources, network
│   ├── ozone.yml
│   ├── kafka.yml
│   ├── flink.yml
│   ├── iceberg-rest.yml
│   ├── trino.yml
│   └── simulator.yml
├── platform/                       # CẤU HÌNH/CODE CÁC THÀNH PHẦN HỆ THỐNG
│   ├── ozone/                       # Ozone component notes; cluster via compose/ozone.yml
│   ├── kafka/                       # Kafka component notes; broker via compose/kafka.yml
│   ├── flink/                       # Dockerfile, Flink SQL, init/smoke config
│   ├── iceberg/                     # Lakekeeper warehouse JSON
│   ├── trino/                       # Iceberg connector/catalog config
│   ├── observability/               # Future Grafana/Prometheus (chưa implement)
│   └── superset/                    # Scope note (không có source Superset trong ZIP)
├── simulator/
│   ├── ami/                         # AMI Kafka event generator (active)
│   └── smart_meter_producer.py      # Legacy/alternate producer; không phải default
├── lakehouse/                      # Bronze/Silver/Gold SQL/data model roadmap
├── benchmarks/                     # CODE CHẠY BENCHMARK, PHÂN THEO THÍ NGHIỆM
│   ├── ingestion/                   # AMI ramp 1K/2K/5K
│   ├── metadata/                    # Streaming metadata/file growth
│   ├── read/                        # Trino fixed-snapshot reads
│   ├── recovery/                    # DataNode failure/recovery
│   ├── concurrent-rw/               # Methodology; chưa có completed runner riêng
│   ├── storage-comparison/          # Ozone vs HDFS/MinIO: scope, chưa có source VDP
│   ├── evidence/local-docker-sep-2026/   # Historical raw runs; KHÔNG coi là final result
│   └── results/                    # Generated runs, Git-ignored
├── scripts/                        # Stable PowerShell entrypoints (local Docker)
├── deployments/vdp-ambari/         # Trạng thái/giới hạn source VDP, không chứa hotfix vendor
├── docs/                           # Architecture, experiment caveats, source provenance
├── schemas/                        # Future multi-source schemas (chưa triển khai)
└── tests/                          # Offline smoke tests, fixtures
```

## Chạy local Docker lab (PowerShell)

Yêu cầu: Windows PowerShell/PowerShell 7, Docker Desktop + Docker Compose plugin, Python 3.9+. Phiên bản image được chốt trong các Compose file; không đánh đồng với cluster Ozone 2.2.1/Ambari riêng.

```powershell
Copy-Item .env.example .env
# MỞ .env, thay mọi placeholder bằng secret riêng.
# S3_ACCESS_KEY/S3_SECRET_KEY phải là cặp key đã đăng ký ở Ozone S3G lab.

python .\scripts\render-local-config.py
docker compose config --quiet
.\scripts\start-safe.ps1
```

**Chú ý:** script `start-safe.ps1` render private config, khởi động services và tạo Bronze database, nhưng bản code ZIP gốc **chưa tự submit Bronze Flink job**. Hãy kiểm tra Flink UI và chỉ submit khi không có job Bronze `RUNNING`:

```powershell
.\scripts\submit-bronze.ps1
.\scripts\run-ami-simulator.ps1 -Mode custom -TargetRate 1 -Duration 10
```

UI lab: Recon `http://localhost:9888`, Flink `http://localhost:8081`, Trino `http://localhost:8080`, Lakekeeper `http://localhost:8181`. Kafka broker là protocol ở `localhost:9092`, **không** phải Kafka UI.

Đừng chạy benchmark tải cao trên stack chưa HEALTHY. Lab Docker gốc được thiết kế cho laptop WSL2 khoảng 10 GiB RAM / 6 CPU và **không đại diện cho production HA** (1 OM, 1 SCM).

## Benchmark (local Docker)

```powershell
# Ingestion; default stage 1K -> 2K -> 5K, nên giảm tham số trên laptop
.\scripts\run-ami-ramp-benchmark.ps1 -StageSeconds 30

# Metadata/file growth với target EPS cố định
.\scripts\run-lakehouse-metadata-growth-benchmark.ps1 -Stages 2 -StageSeconds 60 -TargetRate 50

# Read-only, dùng snapshot ID THỰC của bảng Iceberg
.\scripts\run-trino-read-benchmark.ps1 -SnapshotId 1234567890123456789 -DurationSeconds 60

# DataNode fault injection: CÓ TÁC ĐỘNG; chỉ chạy trên lab tách biệt
.\scripts\run-datanode-leader-failure-test.ps1 -TargetDataNode datanode2
```

**Snapshot ID trong ví dụ là placeholder**; cần truy vấn `lakehouse.bronze."ami_meter_raw$snapshots"` để lấy ID hợp lệ. Trino runner được tạo lại trong lần tổng hợp này để thay script đặt nhầm tên; mới qua **offline tests**, chưa được xác nhận chạy thành công trên Docker của bạn. `benchmarks/README.md` giải thích output, mức độ tin cậy và hạn chế so sánh.

## Trạng thái tính năng

| Thành phần | Trạng thái code trong repository |
|---|---|
| Compose Kafka/Flink/Ozone/Iceberg REST/Trino | Có cấu hình từ archive gốc; chưa deploy lại trong môi trường này |
| AMI simulator + Bronze Iceberg | Có code; duplicate injection đã được bổ sung |
| Bronze Kafka provenance, parse status | Có Flink SQL |
| Silver, Gold, GIS/OMS/SCADA/Weather/DER | Chỉ có vị trí phát triển; **chưa có logic thực** |
| AMI ramp + metadata growth benchmark | Có Python runner gốc |
| DataNode failure test | Có PowerShell runner gốc (tác động đến DataNode) |
| Trino fixed-snapshot read | Có runner mới, chưa live-validated |
| Ozone vs HDFS vs MinIO trên VDP | **Không có source benchmark tương ứng trong ZIP** |
| Flink CEP, Fleet simulator, Superset VDP | Có mô tả trong context dự án, **không có source trong ZIP** |

## Bảo mật & nguồn

- `.env` gốc **không được đưa vào repo**; chỉ `.env.example` với placeholder. Các config có S3 credential gốc đã đổi thành `.template`; `scripts/render-local-config.py` tạo file local Git-ignored từ `.env`. Phải dùng cặp S3 key/secret đúng với Ozone S3G đã provision ở lab, và **không** commit credential thật.
- Không đưa các tài liệu nội bộ MDIS/VDP, IP/host inventory, thông tin tài khoản/khóa của cluster vào repository.
- `docs/source-manifest.csv` đối chiếu tên/sha256 của từng file được lấy từ ZIP gốc, `benchmarks/evidence/` là các log/run lịch sử chưa được xác minh độc lập ở bước đóng gói.
- Chưa có license được chủ sở hữu chỉ định: không tự giả định quyền tái sử dụng cho bên thứ ba.

**Repo này được chuẩn bị để đưa lên GitHub/GitLab; chưa được xuất bản lên dịch vụ Git từ phiên này.**
