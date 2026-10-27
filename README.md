# Streaming Lakehouse trên Apache Ozone

Hệ thống streaming lakehouse thu thập dữ liệu công tơ AMI qua Kafka, xử lý bằng Flink SQL và lưu trữ dưới dạng bảng Apache Iceberg trên Apache Ozone. Lakekeeper quản lý catalog, Trino phục vụ truy vấn dữ liệu.

## Kiến trúc

```text
AMI simulator ──> Kafka ──> Flink SQL ──> Iceberg Bronze ──> Ozone S3G ──> Ozone
                                          │
                                          └── Lakekeeper (REST Catalog + PostgreSQL)

Trino ──> Lakekeeper/Iceberg metadata ──> Parquet/metadata trên Ozone
```

- **Kafka** tiếp nhận sự kiện từ AMI simulator.
- **Flink SQL** xử lý luồng dữ liệu và ghi vào bảng Iceberg Bronze.
- **Apache Iceberg** quản lý cấu trúc bảng, snapshot và metadata.
- **Lakekeeper** cung cấp REST Catalog, sử dụng PostgreSQL để lưu thông tin catalog.
- **Apache Ozone** lưu trữ file Parquet, metadata Iceberg và checkpoint Flink.
- **Trino** truy vấn bảng Iceberg qua Lakekeeper và đọc dữ liệu trên Ozone.

## Cấu trúc thư mục

```text
.
├── compose.yaml                    # Cấu hình Docker Compose chính
├── compose/                        # Cấu hình dịch vụ, tài nguyên và mạng
│   ├── ozone.yml
│   ├── kafka.yml
│   ├── flink.yml
│   ├── iceberg-rest.yml
│   ├── trino.yml
│   └── simulator.yml
├── platform/                       # Cấu hình các thành phần hệ thống
│   ├── ozone/                      # Tài liệu thành phần Ozone
│   ├── kafka/                      # Tài liệu thành phần Kafka
│   ├── flink/                      # Dockerfile, Flink SQL và cấu hình khởi tạo
│   ├── iceberg/                    # Cấu hình warehouse Lakekeeper
│   └── trino/                      # Cấu hình connector và catalog Iceberg
├── simulator/
│   ├── ami/                        # Bộ sinh sự kiện AMI cho Kafka
│   └── smart_meter_producer.py      # Bộ sinh dữ liệu công tơ
├── lakehouse/                      # Tài liệu mô hình dữ liệu Bronze/Silver/Gold
├── benchmarks/                     # Kịch bản đo hiệu năng
│   ├── ingestion/                  # Đo tốc độ nạp dữ liệu AMI
│   ├── metadata/                   # Đo tăng trưởng metadata và file
│   ├── read/                       # Đo truy vấn Trino trên snapshot cố định
│   ├── recovery/                   # Kịch bản phục hồi DataNode
│   ├── evidence/                   # Log và dữ liệu các lần chạy
│   └── results/                    # Kết quả benchmark được sinh khi chạy
├── scripts/                        # Script PowerShell vận hành local Docker
├── deployments/vdp-ambari/          # Tài liệu triển khai VDP/Ambari
├── docs/                           # Tài liệu kiến trúc và thí nghiệm
└── tests/                          # Smoke test và dữ liệu kiểm thử
```

## Chạy local Docker lab (PowerShell)

Yêu cầu: Windows PowerShell hoặc PowerShell 7, Docker Desktop với Docker Compose plugin và Python 3.9+. Môi trường local sử dụng khoảng 10 GiB RAM và 6 CPU trên WSL2. Phiên bản image được khai báo trong thư mục `compose/`.

```powershell
Copy-Item .env.example .env
# Điền thông tin kết nối và khóa truy cập trong .env.
# Sử dụng S3_ACCESS_KEY/S3_SECRET_KEY đã đăng ký trên Ozone S3G.

python .\scripts\render-local-config.py
docker compose config --quiet
.\scripts\start-safe.ps1
```

Script `start-safe.ps1` tạo cấu hình local, khởi động các dịch vụ và khởi tạo database Bronze. Sau khi các dịch vụ sẵn sàng, mở Flink UI để kiểm tra job Bronze. Nếu cần khởi chạy job, thực hiện:

```powershell
.\scripts\submit-bronze.ps1
```

Gửi dữ liệu AMI vào Kafka:

```powershell
.\scripts\run-ami-simulator.ps1 -Mode custom -TargetRate 1 -Duration 10
```

| Dịch vụ | Địa chỉ |
|---|---|
| Ozone Recon | http://localhost:9888 |
| Flink UI | http://localhost:8081 |
| Trino | http://localhost:8080 |
| Lakekeeper | http://localhost:8181 |
| Kafka broker | `localhost:9092` |

## Benchmark (local Docker)

Chạy benchmark sau khi các dịch vụ ở trạng thái sẵn sàng và job Bronze đang `RUNNING`. Điều chỉnh tốc độ phát dữ liệu và thời gian chạy theo tài nguyên của máy.

```powershell
# Đo ingestion theo các mức tải 1K -> 2K -> 5K sự kiện/giây
.\scripts\run-ami-ramp-benchmark.ps1 -StageSeconds 30

# Metadata/file growth với target EPS cố định
.\scripts\run-lakehouse-metadata-growth-benchmark.ps1 -Stages 2 -StageSeconds 60 -TargetRate 50

# Đo truy vấn trên snapshot cố định; thay bằng snapshot ID của bảng
.\scripts\run-trino-read-benchmark.ps1 -SnapshotId 1234567890123456789 -DurationSeconds 60

# Thử nghiệm phục hồi bằng cách gây gián đoạn DataNode trong lab riêng
.\scripts\run-datanode-leader-failure-test.ps1 -TargetDataNode datanode2
```

Lấy snapshot ID cho benchmark truy vấn bằng SQL sau trong Trino, rồi dùng giá trị trả về cho tham số `-SnapshotId`:

```sql
SELECT snapshot_id, committed_at
FROM lakehouse.bronze."ami_meter_raw$snapshots"
ORDER BY committed_at DESC;
```

Xem cách tổ chức kịch bản và kết quả tại [benchmarks/README.md](benchmarks/README.md).

## Cấu hình

Thông tin kết nối được quản lý trong `.env`. Script `scripts/render-local-config.py` đọc các giá trị này để tạo cấu hình local từ các file `.template`. Các file cấu hình được sinh và kết quả benchmark được loại khỏi Git qua `.gitignore`.

Sử dụng `.env.example` làm mẫu khi thiết lập môi trường mới và giữ thông tin xác thực trong cấu hình local.
