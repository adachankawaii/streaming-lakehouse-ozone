# Changelog

## 2026-10-27 — Git-ready consolidation

- Giữ Compose entrypoint tại root, gom source hệ thống vào `platform/{flink,iceberg,trino,ozone,kafka}` và sửa mount/build paths tương ứng.
- Chuyển Python benchmark từ `scripts/benchmark/` sang `benchmarks/{ingestion,metadata}/` và giữ các PowerShell wrappers tại `scripts/`.
- Chuyển file fault-injection vào `benchmarks/recovery/` và tạo wrapper tương thích.
- **Thay** `scripts/run-trino-read-benchmark.ps1` bị đặt sai nội dung bằng wrapper mới + `benchmarks/read/trino_read_benchmark.py`; đây là code mới, chưa live-tested.
- Bổ sung duplicate injection cho `simulator/ami/ami_simulator.py` (tái gửi đúng key/payload, giữ target EPS) và test offline.
- Loại bỏ `.env` và S3 credential demo khỏi bản source; thay bằng `.env.example`, 3 config templates + local renderer; ignore runtime outputs.
- Chuyển toàn bộ historical CSV/JSON/log/TXT từ ZIP sang `benchmarks/evidence/local-docker-sep-2026/`; bỏ nested ZIP trùng lặp.
- Ghi rõ phần VDP/Ambari, CEP, Superset, HDFS/MinIO source **chưa có trong archive**; không giả lập triển khai.

Chưa chạy Docker Compose/Trino/Ozone live trong môi trường biên soạn này; chỉ kiểm thử offline và static checks.
