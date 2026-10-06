import os
from pyspark.sql import SparkSession, functions as F

e = os.environ
cols, files, rpf = int(e["TOTAL_COLUMNS"]), int(e["NUM_FILES"]), int(e["ROWS_PER_FILE"])
path = f"s3a://{e['BUCKET']}/{e['DATA_PATH']}"

spark = SparkSession.builder.appName(f"gen-{cols}cols-{files}files").getOrCreate()

# c0 long, c1 double, c2 string, c3 long ... (random => nén không sụp về 0)
def col(i):
    r = F.rand(i)
    return [(r * 1e9).cast("long"), r, F.md5((r * 1e9).cast("string"))][i % 3].alias(f"c{i}")

# range với numPartitions=files -> mỗi partition ghi đúng 1 file
df = spark.range(0, files * rpf, numPartitions=files).select(*[col(i) for i in range(cols)])
df.write.mode("overwrite").parquet(path)

jvm = spark._jvm
fs = jvm.org.apache.hadoop.fs.Path(path).getFileSystem(spark._jsc.hadoopConfiguration())
st = [s for s in fs.listStatus(jvm.org.apache.hadoop.fs.Path(path)) if s.getPath().getName().endswith(".parquet")]
total = sum(s.getLen() for s in st)
print(f"\n=== GEN DONE: {path}\n files={len(st)} rows={files*rpf} cols={cols} "
      f"total={total/2**20:.1f}MB avg_file={total/len(st)/2**20:.1f}MB "
      f"row_group_size={int(e['ROW_GROUP_SIZE_MB'])}MB compression={e['COMPRESSION']}")
spark.stop()
