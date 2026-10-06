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
- The row-group layout was inspected after the fact with `pyarrow` (footer only): 34 row groups, see "Worked example". Read-side I/O requests to MinIO were not captured, so the S3A request pattern below is derived from source, not measured.
- Sizes printed as `MB` by `gen.py`/`read.py` are MiB (divided by 2^20). The 2 GB file is 2,238,410,503 bytes = 2134.7 MiB.

## How Spark reads Parquet

Everything below is based on Spark 3.5.3 / parquet-mr 1.13.1 source (references in the next section) and on the real file produced by Test 2.

### 1. Granularity: what is the unit at each level

A Parquet file is `file -> row groups -> column chunks -> pages`, plus a footer at the end holding the metadata of all of them.

| Level | Unit | Who decides it | Role in reading |
|---|---|---|---|
| Split | byte range `[start, start+length)` of a file | Spark (`maxSplitBytes`) | One split (or several small files packed together) = one Spark partition = one task |
| Row group | horizontal slice of rows, written by `parquet.block.size` | Writer | Unit of ownership (a row group is read by exactly one task) and unit of filtering (min/max stats, dictionary, bloom filter) |
| Column chunk | all values of one column inside one row group, stored contiguously | Writer | Unit of I/O: the reader fetches a whole chunk of every selected column, never part of a chunk |
| Page | ~`parquet.page.size` (1 MiB default) of one column chunk | Writer | Unit of decompression and decoding; pages are decompressed lazily one at a time |
| Batch | `spark.sql.parquet.columnarReaderBatchSize` rows (4096) | Spark | Unit handed to the rest of the plan by the vectorized reader |

So the answer to "row group or column chunk?": **Spark assigns work by row group** (through byte ranges and the midpoint rule), **and the reader fetches data by column chunk** (only the selected columns, each chunk in full). Neither level is split further across tasks.

### 2. What one task does

1. Receives a `FilePartition` (one or more `PartitionedFile(path, start, length)`).
2. Reads the **footer** of the file (a tail read, plus a seek back) and keeps only the row groups whose midpoint is inside `[start, start+length)` ([`ParquetFooterReader.readFooter`](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/ParquetFooterReader.java#L52-L66)).
3. Applies **row-group filters** if a predicate was pushed down (`spark.sql.parquet.filterPushdown`): min/max statistics, then dictionary, then bloom filter; surviving row groups only ([`ParquetFileReader.filterRowGroups`](https://github.com/apache/parquet-mr/blob/apache-parquet-1.13.1/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetFileReader.java#L883-L900)). Without a predicate nothing is skipped.
4. Clips the schema to the selected columns (column pruning).
5. Loops over its row groups **sequentially in one thread**. For each one, `VectorizedParquetRecordReader.checkEndOfRowGroup` calls `ParquetFileReader.readNextRowGroup` ([L416-L430](https://github.com/apache/spark/blob/v3.5.3/sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/VectorizedParquetRecordReader.java#L416-L430)), which:
   - lists the selected column chunks of the row group in file order;
   - merges chunks that are **physically adjacent** into one `ConsecutivePartList`, a new list starts when `currentParts.endPos() != startingPos` ([`internalReadRowGroup`](https://github.com/apache/parquet-mr/blob/apache-parquet-1.13.1/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetFileReader.java#L970-L992));
   - for each list does one `seek(offset)` and reads the whole run into heap buffers of at most `parquet.read.allocation.size` (8 MiB) with `readFully` ([`ConsecutivePartList.readAll`](https://github.com/apache/parquet-mr/blob/apache-parquet-1.13.1/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetFileReader.java#L1832-L1855)).
6. Decodes page by page: `ColumnChunkPageReader.readPage` decompresses one page at a time ([`ColumnChunkPageReadStore` L120-L139](https://github.com/apache/parquet-mr/blob/apache-parquet-1.13.1/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ColumnChunkPageReadStore.java#L120-L139)); `VectorizedColumnReader.readBatch` fills column vectors of 4096 rows.
7. Moves to the next row group when `rowsReturned == totalCountLoadedSoFar`.

Consequences: parallelism exists **across tasks** only; inside a task the I/O is serial and a whole row group's selected chunks are in memory at once.

### 3. Formulas

```
defaultParallelism   = spark.default.parallelism            (if set)
                       else max(total registered executor cores, 2)      # standalone
minPartitionNum      = spark.sql.files.minPartitionNum      (if set)
                       else spark.sql.leafNodeDefaultParallelism          # default = defaultParallelism
totalBytes           = sum over files of (fileLength + openCostInBytes)
bytesPerCore         = totalBytes / minPartitionNum
maxSplitBytes        = min(maxPartitionBytes, max(openCostInBytes, bytesPerCore))

splits of a file     = offsets 0, maxSplitBytes, 2*maxSplitBytes, ...   -> ceil(fileLength / maxSplitBytes) ranges
tasks                = bin-packing of all ranges, sorted by length descending:
                         if currentSize + range.length > maxSplitBytes: close partition
                         currentSize += range.length + openCostInBytes

row group r belongs to the split containing   midpoint(r) = startOfFirstChunk(r) + totalCompressedSize(r) / 2

bytes fetched per row group = sum of totalCompressedSize of the SELECTED column chunks  (+ footer once per task)
reads per row group         = number of physically contiguous runs of selected chunks
buffers per run             = ceil(runLength / parquet.read.allocation.size)

concurrent tasks            = min(tasks, floor(executorCores / spark.task.cpus) * executors granted)
```

Notes:
- `bytesPerCore` only matters when data is small relative to the cluster; otherwise `maxPartitionBytes` wins.
- `openCostInBytes` is a packing penalty: tiny files count as at least 4 MiB each, so they are not packed into one huge task. It does not change how a file larger than `maxSplitBytes` is cut.
- The midpoint rule makes row-group ownership exact, but the number of row groups per task is only about `maxSplitBytes / rowGroupSize`; it can be 0, 1, 2, ...

### 4. Parameter reference

Spark SQL (read side)

| Parameter | Default | Effect |
|---|---|---|
| `spark.sql.files.maxPartitionBytes` | 128 MiB | Upper bound of a split. Smaller = more tasks. Compared against row-group size, not against file count |
| `spark.sql.files.openCostInBytes` | 4 MiB | Per-file cost used in packing and in `totalBytes` |
| `spark.sql.files.minPartitionNum` | unset | Suggested minimum partitions; overrides `leafNodeDefaultParallelism` in `bytesPerCore` |
| `spark.sql.leafNodeDefaultParallelism` | `sc.defaultParallelism` | Divisor for `bytesPerCore` |
| `spark.default.parallelism` | total cores (min 2) | Feeds `leafNodeDefaultParallelism` |
| `spark.sql.parquet.enableVectorizedReader` | true | Batch (columnar) decoding instead of row-by-row |
| `spark.sql.parquet.columnarReaderBatchSize` | 4096 | Rows per `ColumnarBatch` |
| `spark.sql.parquet.filterPushdown` | true | Push predicates to row-group filtering |
| `spark.sql.parquet.recordLevelFilter.enabled` | false | Also filter individual records inside surviving row groups |

parquet-mr (read side, set as `spark.hadoop.*` in Spark)

| Parameter | Default | Effect |
|---|---|---|
| `parquet.read.allocation.size` | 8 MiB | Size of each heap buffer used to read a run of chunks |
| `parquet.filter.stats.enabled` | true | Row-group pruning by min/max |
| `parquet.filter.dictionary.enabled` | true | Row-group pruning by dictionary |
| `parquet.filter.bloom.enabled` | true | Row-group pruning by bloom filter |
| `parquet.filter.columnindex.enabled` | true | Page-level skipping with the column index |

Write side (decide the layout the reader sees)

| Parameter | Default | Effect |
|---|---|---|
| `parquet.block.size` | 128 MiB | Row-group target. The writer flushes when buffered (encoded) size is within ~2 records of it ([`checkBlockSizeReached`](https://github.com/apache/parquet-mr/blob/apache-parquet-1.13.1/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/InternalParquetRecordWriter.java#L150-L172)), so on-disk row groups are about this size |
| `parquet.page.size` | 1 MiB | Page target, i.e. decompression/decoding unit |
| `spark.sql.parquet.compression.codec` | snappy | Chunk compression |

S3A (object-store I/O)

| Parameter | Default | Effect |
|---|---|---|
| `fs.s3a.readahead.range` | 64 KiB | Forward seeks shorter than this are served by skipping bytes in the open HTTP stream; longer ones close and reopen the stream |
| `fs.s3a.experimental.input.fadvise` | `normal` | `normal`: open-ended range to EOF until the first backward seek, then `random`; `random`: range = `max(readahead, length)` per request; `sequential`: range to EOF |
| `fs.s3a.connection.maximum` | 96 | HTTP connection pool shared by all streams in the JVM |

Cluster

| Parameter | Effect |
|---|---|
| `spark.executor.cores`, `spark.task.cpus` | Slots per executor = `executor.cores / task.cpus` |
| `spark.cores.max` | Total cores the app may take in standalone mode (this lab: `NUM_EXECUTORS * EXECUTOR_CORES`) |
| `spark.executor.memory` | Must fit one row group's selected chunks per concurrent task, plus decoded batches |

### 5. Worked example (the real 2 GB file from Test 2)

Layout from the footer (`pyarrow`):

| Fact | Value |
|---|---|
| File length | 2,238,410,503 B |
| Row groups | 34 (33 x 143,614 rows + 1 x 60,738 rows) |
| Row group size on disk | ~66,955,000 B (63.86 MiB) compressed; 74,686,8xx B uncompressed |
| Writer setting | `parquet.block.size` = 67,108,864 B (64 MiB); flush is checked on encoded bytes, so on-disk size is just below it |
| Column chunks in row group 0 | c0 INT64 865,912 B, c1 DOUBLE 1,149,232 B, c2 BYTE_ARRAY 4,680,409 B, c3 INT64 865,938 B, c4 DOUBLE 1,149,232 B, ... c5 starts at byte 8,710,727 |

**Step 1, split size.** `totalBytes = 2,238,410,503 + 4,194,304 = 2,242,604,807`; `bytesPerCore = 2,242,604,807 / 8 = 280,325,600` (267 MiB); `maxSplitBytes = min(134,217,728, 280,325,600) = 134,217,728` (128 MiB).

**Step 2, splits and tasks.** `ceil(2,238,410,503 / 134,217,728) = 17` ranges; one file, so no packing: 17 tasks (observed 17).

**Step 3, row-group ownership** (midpoint rule, computed from the footer):

| Split (task) | Byte range | Row groups |
|---|---|---|
| 0 | 0 to 134,217,728 | 0, 1 |
| 1 | 134,217,728 to 268,435,456 | 2, 3 |
| ... | ... | 2 per split |
| 15 | 2,013,265,920 to 2,147,483,648 | 30, 31 |
| 16 | 2,147,483,648 to 2,238,410,503 | 32, 33 (last, short) |

Each task owns exactly 2 row groups (34 / 17), because 128 MiB is about 2 x 64 MiB.

**Step 4, selected columns c0..c4 (`READ_COLUMNS=5`).** In each row group they are one contiguous byte run: `[4, 8,710,727)` for row group 0 = 8,710,723 B, followed immediately by c5 (unselected). Therefore:
- reads per row group: 1 (a single `seek` + `readFully`);
- buffers: `8,710,723 / 8,388,608` = 1 full 8 MiB buffer + 322,115 B;
- bytes per row group: 8,710,723 B = 13.0% of the row group, because c2 (string, 4.68 MB) is far larger than the long/double chunks;
- the other 25 chunks of each row group (~58 MB) are never requested.

**Step 5, total bytes.** Sum of the selected chunks over 34 row groups = 291,139,081 B = 277.65 MiB. Spark reported `input=279.7MB` (MiB). The ~2 MiB difference is plausibly footer and metadata reads by 17 tasks [INFERENCE: not measured].

**Step 6, what a task does at runtime.** Task 0: read footer, keep row groups 0 and 1, read bytes `[4, 8,710,727)`, decode in 4096-row batches, then seek forward ~58 MB past the unselected chunks to row group 1's selected run at `66,957,309`, read it, finish.

### 6. What changes the numbers (derived from the formulas, not measured)

| Change | Effect on this file |
|---|---|
| `MAX_PARTITION_MB=64` | `maxSplitBytes` = 64 MiB; 34 splits, about 1 row group per task; concurrency still capped by 8 slots |
| `MAX_PARTITION_MB=32` | `maxSplitBytes` = 32 MiB; 67 splits but only 34 row groups: about half the tasks own 0 row groups (a row group is never split) |
| `MAX_PARTITION_MB=256` | `min(256, 267)` = 256 MiB; 9 splits, about 4 row groups per task; 9 tasks on 8 slots, so the last task waits for a free slot |
| `ROW_GROUP_SIZE_MB=256` at write time | 9 row groups, 128 MiB splits: many splits contain no midpoint, so effective parallelism is about 9, not 17 |
| `ROW_GROUP_SIZE_MB=8` at write time | ~270 row groups, many small chunks; more footer metadata, more seeks, finer packing |
| `READ_COLUMNS=30` | One contiguous run covering the whole row group (~67 MB, 8 buffers of at most 8 MiB) per row group; ~2.2 GB read |
| Select c0, c5 only | Two separate runs per row group (not adjacent): 2 seeks and 2 reads per row group |
| `NUM_EXECUTORS=2` (4 cores) | Same 17 tasks, at most 4 run concurrently |

### 7. S3A request pattern (derived from source, not captured)

`S3AInputStream` keeps one HTTP range stream per open file. With the default `normal` policy ([`seekInStream`](https://github.com/apache/hadoop/blob/rel/release-3.3.4/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AInputStream.java#L287-L345), [`calculateRequestLimit`](https://github.com/apache/hadoop/blob/rel/release-3.3.4/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AInputStream.java#L830-L860)):
- The first open requests a range to the end of the object.
- A forward seek shorter than `max(readahead, bytes available)` (64 KiB default) skips inside the open stream. The ~58 MB gap between selected runs of consecutive row groups is far larger, so the stream is closed and a new ranged GET is issued for the next run.
- A backward seek switches the stream to `random`, after which each request is `[pos, pos + max(readahead, length))`.

Practical reading: with column pruning, expect about one ranged GET per contiguous run of selected chunks per row group, plus footer reads. To confirm, enable MinIO audit/trace (`mc admin trace`) while running `make read`. Comparing `fs.s3a.experimental.input.fadvise=random` vs `normal` via `spark.hadoop.fs.s3a.experimental.input.fadvise` is an obvious next experiment; no result is claimed here.

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
