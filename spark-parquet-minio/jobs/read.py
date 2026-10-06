"""Read the dataset and report read parallelism.

After the query, task launch/duration data is pulled from the driver REST API
(localhost:4040) to compute executors used and peak concurrent tasks per stage.
"""
import json, os, time, urllib.request
from pyspark.sql import SparkSession, functions as F

e = os.environ
n = int(e["READ_COLUMNS"])
path = f"s3a://{e['BUCKET']}/{e['DATA_PATH']}"

spark = SparkSession.builder.appName(f"read-{n}cols").getOrCreate()
sc = spark.sparkContext

df = spark.read.parquet(path)
total_cols = len(df.columns)
cols = df.columns[:n]
q = df.select(*cols).agg(*[F.max(c).alias(c) for c in cols])  # max() forces each selected column to actually be read

t = time.time()
sc.setJobDescription(f"read {n}/{total_cols} cols")
q.collect()
dt = time.time() - t

# --- parallelism analysis via the driver REST API ---
api = f"http://localhost:4040/api/v1/applications/{sc.applicationId}"
get = lambda p: json.load(urllib.request.urlopen(api + p))
stages = [s for s in get("/stages?details=false") if s["status"] == "COMPLETE" and s["inputBytes"] > 0]
print(f"\n=== READ DONE: {n}/{total_cols} cols, wall={dt:.1f}s")
for s in stages:
    tasks = get(f"/stages/{s['stageId']}/{s['attemptId']}/taskList?length=100000")
    ev = []
    for t_ in tasks:
        st = time.mktime(time.strptime(t_["launchTime"][:19], "%Y-%m-%dT%H:%M:%S")) + float(t_["launchTime"][20:23]) / 1000
        ev += [(st, 1), (st + t_["duration"] / 1000, -1)]
    cur = peak = 0
    for _, d in sorted(ev):
        cur += d; peak = max(peak, cur)
    execs = {t_["executorId"] for t_ in tasks}
    print(f" stage {s['stageId']}: tasks={len(tasks)} executors_used={len(execs)} "
          f"peak_concurrent_tasks={peak} input={s['inputBytes']/2**20:.1f}MB "
          f"executorRunTime={s['executorRunTime']/1000:.1f}s")
print(f" total_task_slots={sc.defaultParallelism}")

hold = int(e.get("HOLD_SECONDS", 0))
if hold:
    print(f" holding {hold}s, Spark UI: http://localhost:4040")
    time.sleep(hold)
spark.stop()
