# Trạng thái kiểm thử tại thời điểm đóng gói

- Đã kiểm kê archive ZIP gốc và ghi SHA256 cho 142 file nguồn/evidence được đưa sang repo (`source-manifest.csv`). `.env` gốc, runner Trino gắn nhầm tên và các nested ZIP trùng lặp được loại khỏi package.
- Đã chạy `python -m compileall` cho Python runner và simulator.
- Đã chạy 8 offline unit tests, gồm đường dẫn Compose, duplicate events cùng payload, schema SQL Trino pinned snapshot, pagination và lưu 3 output của benchmark mock.
- Đã parse YAML static; **không chạy Docker Engine, không gọi Kafka/Flink/Ozone/Trino thật, không tiếp cận cluster VDP** từ phiên tổng hợp.
- Chưa xác minh lại việc login/secret tương thích với Ozone S3G sau khi thay `.env`; secret của `.env` mới phải được cấu hình/đăng ký đúng cách trong lab thực tế.
- `.env` gốc chứa credential và không được commit. 3 S3 credential files được chuyển thành template; các file render sẽ ở Git-ignored paths.

`PASS offline` khác `PASS end-to-end`. Mọi thay đổi sinh ra từ bước tổng hợp cần smoke test lại ở lab và freeze commit hash trước khi chạy final benchmark.
