# spark-parquet-minio

Measures how Spark reads Parquet from MinIO over S3A and whether the read is parallel.

Prebuilt images only, nothing is built locally:
- `apache/spark:3.5.3`: standalone master, workers, history server, and the driver for jobs.
- `bitnamilegacy/minio`: MinIO server and `mc` (`minio/minio` is no longer published on Docker Hub).

S3A jars (`hadoop-aws:3.3.4`, `aws-java-sdk-bundle:1.12.262`) are resolved at submit time via `spark.jars.packages` into the `ivy` volume (~280 MB on first run, cached afterwards).

## Usage
```bash
make up     # MinIO, master, workers, history server
make gen    # write Parquet to s3a://$BUCKET/$DATA_PATH
make read   # read N columns, print per-stage parallelism
make clean  # remove containers and volumes
```

| UI | URL |
|----|-----|
| Spark master | http://localhost:8080 |
| Driver (only while a job runs) | http://localhost:4040 |
| History server | http://localhost:18080 |
| MinIO console | http://localhost:9001 (`minioadmin` / `minioadmin`) |

All parameters live in `.env` and can be overridden per run, e.g. `make read READ_COLUMNS=30`. Changing `WORKER_*` requires `make up` again. `HOLD_SECONDS=300 make read` keeps the driver UI up after the query.

## Parameters
| Group | Variable | Effect |
|---|---|---|
| Generate | `TOTAL_COLUMNS` | Column count (long/double/string, cycling) |
| | `NUM_FILES`, `ROWS_PER_FILE` | File count and rows per file; together determine file size |
| | `ROW_GROUP_SIZE_MB` | `parquet.block.size` |
| | `PAGE_SIZE_KB`, `COMPRESSION` | `parquet.page.size`, codec |
| Read | `READ_COLUMNS` | Number of columns selected (column pruning) |
| | `MAX_PARTITION_MB`, `OPEN_COST_MB` | `spark.sql.files.maxPartitionBytes`, `spark.sql.files.openCostInBytes` |
| Cluster | `WORKER_REPLICAS`, `WORKER_CORES`, `WORKER_MEMORY` | Worker count and per-worker resources |
| | `NUM_EXECUTORS`, `EXECUTOR_CORES`, `EXECUTOR_MEMORY`, `DRIVER_MEMORY` | Executor sizing; `spark.cores.max = NUM_EXECUTORS * EXECUTOR_CORES` |

## Interpreting output
`make read` prints, for each stage with input: `tasks`, `executors_used`, `peak_concurrent_tasks`, and `input` (bytes actually read, smaller than the dataset when few columns are selected).

Read parallelism is bounded by `min(number of input splits, total executor cores)`. Splits are derived from file boundaries and `maxPartitionBytes`; for Parquet, a row group is assigned to the split containing its midpoint, so a single large file with multiple row groups can still be read in parallel.

## Notes
- Reference run (8 files x 100k rows x 30 columns, 5 columns read, 4 executors x 2 cores): 8 tasks on 4 executors, 8 concurrent, 46 MB read of 356 MB.
- Docker VM disk exhaustion caused JVM SIGBUS crashes during development; keep free space available for the generated data and the `ivy` volume.
