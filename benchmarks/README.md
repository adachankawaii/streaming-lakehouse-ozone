# Benchmark suites

Các runner trong thư mục này đo các lớp khác nhau; không gộp thành một score duy nhất.

| Suite | Code | Ý nghĩa thực tế |
|---|---|---|
| Ingestion ramp | `ingestion/benchmark_ami_ramp.py` | AMI Kafka input, Flink source, checkpoint, Iceberg commit, Ozone object inventory; pipeline end-to-end |
| Metadata/file growth | `metadata/benchmark_lakehouse_metadata_growth.py` | Tác động khi file/metadata tích lũy ở tốc độ input cố định; không phải microbenchmark riêng OM |
| Trino read | `read/trino_read_benchmark.py` | Read latency Trino/Iceberg/catalog/Ozone trên **fixed snapshot**; code mới, chưa xác nhận live |
| Recovery/failure | `recovery/run-datanode-leader-failure-test.ps1` | Cố ý dừng/restart một Docker DataNode; **không dùng trên cluster dùng chung** |
| Concurrent R/W | `concurrent-rw/README.md` | Thiết kế đo latency read khi ingestion chạy; runner hợp nhất chưa có |
| Storage comparison | `storage-comparison/README.md` | Nêu ranh giới code HDFS/MinIO còn thiếu |

**Tách ba mức bằng chứng:** local Docker runtime (trong archive gốc), lịch sử chạy các lần tháng 09/2026 (trong `evidence/`), và code mới viết khi tổng hợp (`read/` và duplicate injection). Không đồng nhất chúng.

`evidence/local-docker-sep-2026/` được bảo lưu như historical dataset, gồm cả run chưa hoàn thành. Không sử dụng số run cũ làm final report nếu không tái lập environment/manifest; Trino historical manifest từng dùng `localhost:8082`, trong khi Compose này cấu hình `8080`.

**Các biến kiểm soát khi so sánh:** cùng dataset + snapshot (read), same EPS/mode (write), topology/replication, Docker resource limits, cache state, warmup, measurement window, thứ tự lặp và manifest version. Cần phân biệt target EPS với **achieved EPS** và committed Kafka lag với backlog live của Flink. Thử nghiệm bị swap/OOM hoặc file sink lỗi cần đánh dấu invalid thay vì chỉ bỏ sample xấu.

Đường dẫn kết quả chạy mới: `benchmarks/results/<run-id>/` (được Git ignore). Nên lưu đầy đủ manifest, CSV, log, image/config version và Git commit trước khi báo cáo.
