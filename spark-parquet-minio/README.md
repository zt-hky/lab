# spark-parquet-minio

Kiểm tra Spark đọc Parquet trên MinIO (S3A) có song song không. Không build image, chỉ dùng:
`apache/spark:3.5.3` (master/worker/history/driver), `bitnamilegacy/minio` (MinIO + mc; `minio/minio` không còn trên Docker Hub).
Jar S3A (`hadoop-aws`, `aws-java-sdk-bundle`) tải qua `spark.jars.packages` vào volume `ivy` (lần đầu ~280MB).

## Chạy
```bash
make up     # minio + master + workers + history server
make gen    # dump dữ liệu giả vào s3a://lab/parquet/test
make read   # đọc N column, in số task / executor / peak concurrent tasks
make clean  # xoá toàn bộ (kể cả data)
```
UI: Master http://localhost:8080 · Driver (khi job chạy) http://localhost:4040 · History http://localhost:18080 · MinIO http://localhost:9001 (minioadmin/minioadmin)

Override nhanh: `make read READ_COLUMNS=30 NUM_EXECUTORS=2`, hoặc sửa `.env`. Đổi `WORKER_*`/`WORKER_REPLICAS` cần `make up` lại.
Muốn xem Spark UI :4040 lâu hơn: `HOLD_SECONDS=120 make read`.

## Tham số (`.env`)
| Nhóm | Biến | Ý nghĩa |
|---|---|---|
| Dump | `TOTAL_COLUMNS` | số column (long/double/string xen kẽ) |
| | `NUM_FILES`, `ROWS_PER_FILE` | số file và số row/file (quyết định file size) |
| | `ROW_GROUP_SIZE_MB`, `PAGE_SIZE_KB`, `COMPRESSION` | `parquet.block.size`, `parquet.page.size`, codec |
| Đọc | `READ_COLUMNS` | chỉ đọc N column đầu (column pruning) |
| | `MAX_PARTITION_MB`, `OPEN_COST_MB` | `spark.sql.files.maxPartitionBytes/openCostInBytes` – quyết định chia split |
| Cluster | `WORKER_REPLICAS`, `WORKER_CORES`, `WORKER_MEMORY` | số worker, core/ram mỗi worker |
| | `NUM_EXECUTORS`, `EXECUTOR_CORES`, `EXECUTOR_MEMORY`, `DRIVER_MEMORY` | executor (standalone: `spark.cores.max = NUM_EXECUTORS*EXECUTOR_CORES`) |

## Đọc kết quả
`make read` in mỗi stage có đọc input: `tasks`, `executors_used`, `peak_concurrent_tasks`, `input` (byte thực đọc – nhỏ hơn tổng file khi chỉ đọc ít column).
Song song bị chặn bởi `min(số split, tổng core executor)`. Số split ≈ file/row group/`MAX_PARTITION_MB`.
Mẫu mặc định (8 file × 100k row × 30 col, đọc 5 col, 4 executor × 2 core): 8 task, 4 executor, peak 8 đồng thời, input 46MB / 356MB.

Lưu ý: máy Docker nhỏ (OrbStack 8GB, đĩa gần đầy) đã làm JVM crash SIGBUS; mặc định được giữ nhỏ.
