#!/usr/bin/env python3
"""sparkflock ingest: JSON files landing in S3/MinIO -> Postgres/RDS, via Spark.

The same file runs against MinIO + Postgres (local) and S3 + RDS (staging).
Only environment variables change; nothing below knows which one it is on.

  job.py --mode stream   keep running; pick up every new file automatically
  job.py --mode once     process everything pending, then exit (batch / K8s Job)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType, TimestampType)

CORRUPT = "_corrupt_record"

# Streaming file sources need an explicit schema. _corrupt_record catches lines
# that are not valid JSON (or have a wrong type) instead of failing the job.
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", LongType()),
    StructField("event_type", StringType()),
    StructField("amount", DoubleType()),
    StructField("ts", TimestampType()),
    StructField(CORRUPT, StringType()),
])
EVENT_COLS = ["event_id", "user_id", "event_type", "amount", "ts"]

# Spark 3.5.x ships Hadoop 3.3.4; these two jars MUST match that version.
DEFAULT_PACKAGES = ",".join([
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.7.4",
])

# The idempotency guard: replaying a file can never create a second row.
MERGE_EVENTS_SQL = """
INSERT INTO events (event_id, user_id, event_type, amount, ts, source_file)
SELECT event_id, user_id, event_type, amount, ts, source_file FROM events_stage
ON CONFLICT (event_id) DO NOTHING
"""
MERGE_REJECTS_SQL = """
INSERT INTO events_rejects (reject_key, raw, source_file, reason)
SELECT reject_key, raw, source_file, reason FROM events_rejects_stage
ON CONFLICT (reject_key) DO NOTHING
"""


@dataclass(frozen=True)
class Config:
    bucket: str
    prefix: str
    s3_endpoint: str          # empty => real AWS S3 + default credential chain
    s3_access_key: str
    s3_secret_key: str
    jdbc_url: str
    db_user: str
    db_password: str
    packages: str             # empty => jars are already on the classpath (K8s image)
    max_partition_mb: int     # bytes of JSON per task: THE knob that sets parallelism
    jdbc_partitions: int      # concurrent DB connections: the cap that protects RDS
    max_files_per_trigger: int
    trigger_seconds: int

    @staticmethod
    def from_env() -> "Config":
        e = os.environ.get
        ssl = e("DB_SSLMODE", "prefer")
        url = (f"jdbc:postgresql://{e('DB_HOST', 'localhost')}:{e('DB_PORT', '5432')}"
               f"/{e('DB_NAME', 'sparkflock')}?sslmode={ssl}&reWriteBatchedInserts=true")
        return Config(
            bucket=e("S3_BUCKET", "sparkflock-ingest"),
            prefix=e("S3_PREFIX", "incoming/"),
            s3_endpoint=e("S3_ENDPOINT", ""),
            s3_access_key=e("S3_ACCESS_KEY", ""),
            s3_secret_key=e("S3_SECRET_KEY", ""),
            jdbc_url=url,
            db_user=e("DB_USER", "sparkflock"),
            db_password=e("DB_PASSWORD", ""),
            packages=e("SPARK_JARS_PACKAGES", DEFAULT_PACKAGES),
            max_partition_mb=int(e("MAX_PARTITION_MB", "128")),
            jdbc_partitions=int(e("JDBC_PARTITIONS", "4")),
            max_files_per_trigger=int(e("MAX_FILES_PER_TRIGGER", "100")),
            trigger_seconds=int(e("TRIGGER_SECONDS", "5")),
        )

    @property
    def input_path(self) -> str:
        return f"s3a://{self.bucket}/{self.prefix}"

    @property
    def checkpoint_path(self) -> str:
        # On S3, not pod-local disk, so a restarted driver resumes where it stopped.
        return f"s3a://{self.bucket}/_checkpoints/ingest"


def build_spark(cfg: Config) -> SparkSession:
    b = (SparkSession.builder.appName("sparkflock-ingest")
         .config("spark.sql.files.maxPartitionBytes", cfg.max_partition_mb * 1024 * 1024)
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
    if cfg.packages:
        b = b.config("spark.jars.packages", cfg.packages)
    if cfg.s3_endpoint:  # MinIO / any S3-compatible store
        b = (b.config("spark.hadoop.fs.s3a.endpoint", cfg.s3_endpoint)
              .config("spark.hadoop.fs.s3a.path.style.access", "true")
              .config("spark.hadoop.fs.s3a.connection.ssl.enabled",
                      str(cfg.s3_endpoint.startswith("https")).lower())
              .config("spark.hadoop.fs.s3a.access.key", cfg.s3_access_key)
              .config("spark.hadoop.fs.s3a.secret.key", cfg.s3_secret_key))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_sql(spark: SparkSession, cfg: Config, sql: str) -> int:
    """Run one statement on the driver's own JDBC connection (Spark's JDBC
    writer can only append/overwrite; it cannot do ON CONFLICT)."""
    jvm = spark._sc._gateway.jvm  # noqa: SLF001
    # --packages jars live in Spark's classloader, which DriverManager cannot
    # see ("No suitable driver"); instantiate the driver through it directly.
    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    driver = jvm.java.lang.Class.forName("org.postgresql.Driver", True, loader).newInstance()
    props = jvm.java.util.Properties()
    props.setProperty("user", cfg.db_user)
    props.setProperty("password", cfg.db_password)
    conn = driver.connect(cfg.jdbc_url, props)
    try:
        st = conn.createStatement()
        try:
            return int(st.executeUpdate(sql))
        finally:
            st.close()
    finally:
        conn.close()


def write_stage(df: DataFrame, cfg: Config, table: str) -> None:
    (df.coalesce(cfg.jdbc_partitions)  # at most N concurrent connections to the DB
       .write.format("jdbc")
       .option("url", cfg.jdbc_url).option("dbtable", table)
       .option("user", cfg.db_user).option("password", cfg.db_password)
       .option("driver", "org.postgresql.Driver")
       .option("batchsize", 5000).option("truncate", "true")
       .mode("overwrite").save())


def ensure_input_prefix(spark: SparkSession, cfg: Config) -> None:
    """S3 has no folders: an empty prefix does not exist, and Spark's file
    stream refuses to start on a missing path. Create the marker object."""
    jvm = spark._sc._gateway.jvm  # noqa: SLF001
    path = jvm.org.apache.hadoop.fs.Path(cfg.input_path)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())  # noqa: SLF001
    if not fs.exists(path):
        fs.mkdirs(path)


def executor_count(spark: SparkSession) -> int:
    infos = spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()  # noqa: SLF001
    return max(len(infos) - 1, 0)  # the list includes the driver


MAX_RECOVER_FILES = 500             # files re-read whole per batch
MAX_RECOVER_BYTES = 32 * 1024 * 1024  # larger pretty-printed files stay rejected


def recover_multiline(spark: SparkSession, batch_df: DataFrame):
    """A pretty-printed message (one JSON object over many lines) arrives as
    corrupt line fragments. Re-read only those files whole and accept a file
    only if its ENTIRE text is one valid JSON object or array. (Spark's own
    multiLine reader is not used: it silently truncates at the first bad
    token.) Returns (rows, recovered_file_uris); plain JSON-lines files never
    take this path, so they stay splittable."""
    files = [r.source_file for r in batch_df.filter(F.col(CORRUPT).isNotNull())
             .select("source_file").distinct().limit(MAX_RECOVER_FILES).collect()]
    if not files:
        return None, []
    texts = (spark.read.text(files, wholetext=True)  # the kwarg, not .option(): .text() resets it
             .select(F.input_file_name().alias("source_file"), F.col("value"))
             .filter(F.length("value") <= MAX_RECOVER_BYTES).collect())
    clean, items = [], []
    for row in texts:
        try:
            doc = json.loads(row.value)
        except ValueError:
            continue
        docs = doc if isinstance(doc, list) else [doc]
        if not docs or not all(isinstance(d, dict) for d in docs):
            continue
        clean.append(row.source_file)
        items.extend((row.source_file, json.dumps(d)) for d in docs)
    if not clean:
        return None, []
    opts = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT}
    rows = (spark.createDataFrame(items, ["source_file", "doc"])
            .select("source_file", F.from_json("doc", EVENT_SCHEMA, opts).alias("r"))
            .select("source_file", "r.*"))
    return rows, clean


def make_batch_handler(spark: SparkSession, cfg: Config, stats: dict):
    def handle(batch_df: DataFrame, batch_id: int) -> None:
        t0 = time.time()
        partitions = batch_df.rdd.getNumPartitions()
        batch_df.persist()
        try:
            if batch_df.count() == 0:
                return
            recovered, clean_files = recover_multiline(spark, batch_df)
            work = batch_df
            if recovered is not None:
                work = (batch_df.filter(~F.col("source_file").isin(clean_files))
                        .unionByName(recovered))
            total = work.count()
            is_bad = F.col(CORRUPT).isNotNull() | F.col("event_id").isNull()

            good = work.filter(~is_bad).select(*EVENT_COLS, "source_file")
            raw = F.coalesce(F.col(CORRUPT), F.to_json(F.struct(*EVENT_COLS)))
            bad = (work.filter(is_bad).select(
                F.md5(F.concat_ws("|", F.col("source_file"), raw)).alias("reject_key"),
                raw.alias("raw"), "source_file",
                F.when(F.col(CORRUPT).isNotNull(), "unparseable_json")
                 .otherwise("missing_event_id").alias("reason")))

            t_stage = time.time()
            write_stage(good, cfg, "events_stage")
            t_merge = time.time()
            inserted = run_sql(spark, cfg, MERGE_EVENTS_SQL)
            t_done = time.time()
            write_stage(bad, cfg, "events_rejects_stage")
            run_sql(spark, cfg, MERGE_REJECTS_SQL)
            rejected = work.filter(is_bad).count()

            stats["batches"] += 1
            stats["rows_in"] += total
            stats["inserted"] += inserted
            stats["rejected"] += rejected
            stats["max_executors"] = max(stats["max_executors"], executor_count(spark))
            print(f"[batch {batch_id}] rows_in={total} inserted={inserted} "
                  f"dup_skipped={total - rejected - inserted} rejected={rejected} "
                  f"input_partitions={partitions} executors={executor_count(spark)} "
                  f"secs={time.time() - t0:.1f} "
                  f"(stage_write={t_merge - t_stage:.1f} pg_merge={t_done - t_merge:.1f})",
                  flush=True)
        finally:
            batch_df.unpersist()
    return handle


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--mode", choices=["stream", "once"], default="stream")
    args = ap.parse_args(argv)

    cfg = Config.from_env()
    spark = build_spark(cfg)
    run_sql(spark, cfg, (Path(__file__).parent / "schema.sql").read_text())
    ensure_input_prefix(spark, cfg)

    stream = (spark.readStream.schema(EVENT_SCHEMA)
              .option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", CORRUPT)
              .option("maxFilesPerTrigger", cfg.max_files_per_trigger)
              .json(cfg.input_path)
              .withColumn("source_file", F.input_file_name()))

    stats = {"batches": 0, "rows_in": 0, "inserted": 0, "rejected": 0, "max_executors": 0}
    writer = (stream.writeStream
              .foreachBatch(make_batch_handler(spark, cfg, stats))
              .option("checkpointLocation", cfg.checkpoint_path))
    writer = (writer.trigger(availableNow=True) if args.mode == "once"
              else writer.trigger(processingTime=f"{cfg.trigger_seconds} seconds"))

    print(f"watching {cfg.input_path} mode={args.mode} "
          f"maxPartitionMB={cfg.max_partition_mb} jdbcPartitions={cfg.jdbc_partitions}",
          flush=True)
    query = writer.start()
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
    print("SUMMARY " + json.dumps(stats), flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
