# Ozone vs HDFS vs MinIO: code chưa được cung cấp

Thí nghiệm metadata benchmark HDFS/O3FS/OFS, Freon `ommg`, và pipeline so sánh Ozone/HDFS/MinIO trên VDP được nhắc trong tài liệu context/runbook, **nhưng các runner Java/Python/Bash và raw result tương ứng không nằm trong archive source `ozone-demo.zip`**.

Repo này KHÔNG tự dựng lại các lệnh rồi nhận là code thực nghiệm đã chạy. Chỉ bổ sung các source *đã có thật* sau khi thu thập từ cluster và kiểm tra checksum/provenance. Khi đưa lên, phân tách rõ:

```text
storage-comparison/
  hdfs/         # HDFS native metadata/data
  ozone-o3fs/   # Ozone via Hadoop filesystem API
  ozone-ofs/    # Ozone via OFS API
  ozone-s3/     # Ozone S3 Gateway path
  minio-s3/     # MinIO S3 path
  analysis/      # Raw measurements -> comparative tables/plots
```

Đừng so HDFS client RPC trực tiếp với Ozone S3 Gateway rồi kết luận về metadata backend nếu không ghi nhận khác biệt protocol/path và replication policy. Phân biệt microbenchmark metadata vs Iceberg-Flink end-to-end.
