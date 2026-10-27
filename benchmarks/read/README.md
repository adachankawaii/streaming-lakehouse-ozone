# Fixed-snapshot Trino read benchmark

Đây là **runner mới** viết để thay `run-trino-read-benchmark.ps1` gốc bị ghi đè nhầm bằng DataNode failure script. Dùng HTTP Trino Statement API (Python stdlib) và ghi `benchmark_manifest.json`, `queries.csv`, `summary.json`.

**Precheck:** Trino `localhost:8080` trả `/v1/info`, table Bronze đã có Iceberg snapshot hợp lệ, truy vấn metadata `$snapshots` để lấy `snapshot_id`. Không dùng snapshot ID ví dụ trong README chính.

```powershell
.\scripts\run-trino-read-benchmark.ps1 -SnapshotId <ACTUAL_SNAPSHOT_ID> -DurationSeconds 60
```

SQL mặc định là aggregate `SELECT COUNT(*) ... FOR VERSION AS OF <snapshot> WHERE MOD(kafka_offset,100)=residue`. Warmup queries không tính vào bảng latency, mỗi query được chờ hoàn tất trước khi bắt đầu query tiếp theo. `p50/p95/p99` là **wall-clock quan sát từ client**; không phải Ozone read latency. Khi so baseline và concurrent R/W, phải dùng **cùng snapshot ID, SQL, Trino instance, cache policy và environment**.

Chưa có bằng chứng runner mới đã chạy thành công trên Trino 483 trong môi trường thực. `tests/` chỉ kiểm chứng offline logic/format, không thay thế live-validation.
