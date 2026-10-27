# VDP/ODP Ambari cluster (not the Docker environment)

Tài liệu context gần đây mô tả lab VDP 3 host với custom **OZONE221 / Apache Ozone 2.2.1** do Ambari quản lý, RATIS/THREE; Flink + Iceberg/Lakekeeper + Trino và demo AMI/Kafka/Superset đã có triển khai trên máy remote.

**Archive code hiện có trong Project chỉ là `ozone-demo.zip` cho local Docker Compose** (Ozone image 2.2.0), không chứa các file `OZONE221` Ambari stack scripts, Flink CEP/Fleet simulator, Trino/Superset runtime, HDFS/MinIO benchmark Java/shell source từ cluster. Những file đó không thể coi là đã được version hóa trong repo này; cần export từ source-of-truth thực tế sau khi rà soát secret.

Khi đưa vào repo: tách `ambari-stack/OZONE221/`, `config/` (template không secret), `provisioning/` (idempotent), `demo/`, `benchmarks/`, `manifests/`. Không copy `/var/lib/ambari-agent/cache`, credential Kerberos/REST, host IP inventory, hoặc runbook chứa mật khẩu. Không chạy automation fault-injection trên môi trường dùng chung chỉ vì đã có script local.

**Trạng thái:** documentation + placeholder, không phải deployment package đã kiểm thử.
