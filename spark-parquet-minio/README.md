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

## Test results

Environment: OrbStack Docker VM (8 GB RAM), single host, MinIO on the same Docker network, Spark 3.5.3 standalone.
Common settings: 2 workers (4 cores, 2 GB each), `EXECUTOR_CORES=2`, `EXECUTOR_MEMORY=1g`, `NUM_EXECUTORS=4` (8 task slots), `DRIVER_MEMORY=512m`, `ROW_GROUP_SIZE_MB=64`, `PAGE_SIZE_KB=1024`, `COMPRESSION=snappy`, `TOTAL_COLUMNS=30`, `READ_COLUMNS=5`, `MAX_PARTITION_MB=128`, `OPEN_COST_MB=4`.

### Test 1: many small files

Input (`.env`): `NUM_FILES=8`, `ROWS_PER_FILE=100000`. Commands: `make up && make gen && make read`.

Output:
```
=== GEN DONE: s3a://lab/parquet/test
 files=8 rows=800000 cols=30 total=355.8MB avg_file=44.5MB row_group_size=64MB compression=snappy
=== READ DONE: 5/30 cols, wall=4.7s
 stage 1: tasks=8 executors_used=4 peak_concurrent_tasks=8 input=46.3MB executorRunTime=21.5s
 total_task_slots=8
```

### Test 2: one 2 GB file

Input (`.env`): `NUM_FILES=1`, `ROWS_PER_FILE=4800000`. Commands: `make gen && make read` (and `HOLD_SECONDS=900 make read` to keep the driver UI on :4040).

Output of `make gen`:
```
=== GEN DONE: s3a://lab/parquet/test
 files=1 rows=4800000 cols=30 total=2134.7MB avg_file=2134.7MB row_group_size=64MB compression=snappy
```

Output of `make read`, two runs with identical settings:
```
run A  === READ DONE: 5/30 cols, wall=11.5s
       stage 1: tasks=17 executors_used=4 peak_concurrent_tasks=9 input=279.7MB executorRunTime=62.6s
run B  === READ DONE: 5/30 cols, wall=14.8s
       stage 1: tasks=17 executors_used=2 peak_concurrent_tasks=6 input=279.7MB executorRunTime=45.2s
       total_task_slots=8
```

### Findings

| Observation | Conclusion |
|---|---|
| Test 2: 1 file produced 17 tasks on multiple executors | A single Parquet file is read in parallel. |
| 2134.7 MB / 128 MB `maxPartitionBytes` = 16.7, and tasks = 17 | Task count follows `ceil(size / maxPartitionBytes)` here. Each split of 128 MB holds about two 64 MB row groups. Smaller `MAX_PARTITION_MB` gives more tasks, down to one row group per task (a row group is not split). |
| Test 1: 8 files gave 8 tasks, 8 concurrent | Small files give one task per file; concurrency is capped by 8 slots. |
| Input 279.7 MB of 2134.7 MB (13%) for 5 of 30 columns (17%) | Column pruning works: only the selected column chunks are fetched from MinIO. The percentage is below 17% because the 30 columns are not equal in size. |
| Test 1: 46.3 MB of 355.8 MB (13%) | Same pruning ratio as Test 2. |
| Runs A and B: 4 vs 2 executors with identical settings | Executor placement varies between runs. In run B both executors were on one worker (same IP). Parallelism is bounded by granted executor cores, not by the configured `NUM_EXECUTORS`. Check the master UI (:8080) for the executors of the app. [INFERENCE: the second worker had not registered or not been assigned an executor at submit time; not verified.] |
| In run A, the first wave of tasks took 8-10 s, later tasks about 1-3 s | The first tasks pay start-up costs (JVM warm-up, S3A connections, Parquet footer reads). [INFERENCE: not profiled.] |

Reading the numbers:
- `peak_concurrent_tasks` is computed from task launch time and duration at ms resolution. Run A reports 9 with only 8 slots, so treat it as approximate (+/-1).
- Stage 0 in each read is a single task; it is the metadata/schema step, not data scan (the data scan is stage 1). [INFERENCE from task count and duration.]

### Limitations

- One run per configuration, no repetition, so no throughput or timing comparison is claimed.
- MinIO and Spark share one host and one disk, so network and storage latency of a real object store are not represented.
- The actual row-group count in the file was not inspected; it is assumed from `parquet.block.size=64MB`.

## Source code references

Spark `v3.5.3` (the image used here) and parquet-mr `1.13.1` (the Parquet version Spark 3.5.3 depends on). Links are line-pinned permalinks.

Read path, in the order it executes:

1. **Split size.** [`FilePartition.maxSplitBytes`](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FilePartition.scala#L109-L120):
   ```scala
   val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
   val bytesPerCore = totalBytes / minPartitionNum          // defaults to leafNodeDefaultParallelism
   Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
   ```
   `minPartitionNum` falls back to `leafNodeDefaultParallelism`, which defaults to `SparkContext#defaultParallelism` ([`SQLConf` L616-L622](https://github.com/apache/spark/blob/v3.5.3/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L616-L622)). Defaults: `maxPartitionBytes` 128MB ([L1753-L1759](https://github.com/apache/spark/blob/v3.5.3/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1753-L1759)), `openCostInBytes` 4MB ([L1761-L1770](https://github.com/apache/spark/blob/v3.5.3/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1761-L1770)).
2. **Parquet is always splittable.** [`ParquetFileFormat.isSplitable`](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFileFormat.scala#L103-L108) returns `true` unconditionally (it does not depend on the codec, unlike gzip text).
3. **File to byte ranges.** [`PartitionedFileUtil.splitFiles`](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/PartitionedFileUtil.scala#L28-L45) cuts a file into `[offset, offset + maxSplitBytes)` ranges: `(0L until file.getLen by maxSplitBytes)`. It cuts at arbitrary byte offsets, not row-group boundaries.
4. **Ranges to tasks.** [`FileSourceScanExec` L675-L709](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/DataSourceScanExec.scala#L675-L709) sorts splits by size descending and calls [`FilePartition.getFilePartitions`](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FilePartition.scala#L55-L85), which bin-packs ranges greedily (`if (currentSize + file.length > maxSplitBytes) closePartition()`, then `currentSize += file.length + openCostInBytes`). Each resulting `FilePartition` is one `FileScanRDD` partition, i.e. one task.
5. **Byte range to row groups.** The task's range is turned into a footer filter: [`ParquetFooterReader.readFooter` L52-L66](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/ParquetFooterReader.java#L52-L66) uses `.withRange(fileStart, fileStart + file.length())` (the non-vectorized reader does the same in [`SpecificParquetRecordReaderBase` L109](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/SpecificParquetRecordReaderBase.java#L109)). parquet-mr then keeps a row group only if its midpoint lies in the range: [`ParquetMetadataConverter.filterFileMetaDataByMidpoint`](https://github.com/apache/parquet-mr/blob/apache-parquet-1.13.1/parquet-hadoop/src/main/java/org/apache/parquet/format/converter/ParquetMetadataConverter.java#L1244-L1293):
   ```java
   long midPoint = startIndex + totalSize / 2;
   if (filter.contains(midPoint)) { newRowGroups.add(rowGroup); }
   ```
   Every row group has exactly one midpoint, so it is read by exactly one task: no row group is read twice, none is skipped, and a row group is never split across tasks.
6. **Column pruning.** [`ParquetReadSupport.clipParquetSchema` (call at L153)](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetReadSupport.scala#L150-L156) clips the file schema to the requested columns, so the Parquet reader only fetches those column chunks. This is why `input` was about 13% of the file for 5 of 30 columns.

### Checking the formulas against the test results

`defaultParallelism` = 8 (`total_task_slots=8` in the output).

| Test | `totalBytes` | `bytesPerCore` | `maxSplitBytes` | Expected tasks | Observed tasks |
|---|---|---|---|---|---|
| 1: 8 files x 44.5 MB | 8 x (44.5 + 4) = 388 MB | 48.5 MB | min(128, 48.5) = 48.5 MB | 8 (a second 44.5 MB file does not fit: 48.5 + 44.5 > 48.5, so one file per partition) | 8 |
| 2: 1 file x 2134.7 MB | 2134.7 + 4 = 2138.7 MB | 267.3 MB | min(128, 267.3) = 128 MB | ceil(2134.7 / 128) = 17 | 17 |

Consequences for tuning:
- A single file is parallelizable only if `size / maxSplitBytes > 1` and it contains more than one row group. If a split's range contains no row-group midpoint, its task reads nothing.
- With few cores or small data, `bytesPerCore` (not `MAX_PARTITION_MB`) becomes the effective split size, so shrinking the cluster can reduce the task count.
- Row groups larger than `maxSplitBytes` give some tasks zero row groups and others one, so tasks are uneven.

Not covered by this lab: the vectorized reader (`spark.sql.parquet.enableVectorizedReader`, default true) uses the same range-to-row-group mapping through `ParquetFooterReader`; per-column-chunk I/O in parquet-mr (`ParquetFileReader`, S3A seek/read-ahead) was not traced.

## Notes
- Docker VM disk exhaustion caused JVM SIGBUS crashes during development; keep free space available for the generated data and the `ivy` volume.
