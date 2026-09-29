# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver — cast, dedupe, write down every decision
# MAGIC Decisions (with numbers): 1) dedupe on `poll_id` **before** any LAG (2,000 removed) · 2) negative delta = wrap, add 2^32 (992 in / 479 out) · 3) reboot = NULL delta (16) · 4) trim join key + NULLIF rated speed.
# MAGIC Silver keeps the cleaned *polls*; the wrap/reboot maths happens in Gold where the LAG lives.

# COMMAND ----------
# MAGIC %run ./00_config

# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql import Window

bp = spark.table(T("bronze_polls"))
bm = spark.table(T("bronze_master"))

# try_cast, never cast (ANSI mode). in_octets goes to BIGINT: it reaches 4,294,967,295.
typed = bp.selectExpr(
    "device_id", "interface_id",
    "try_cast(poll_ts as timestamp) poll_ts",
    "try_cast(in_octets as bigint) in_octets",
    "try_cast(out_octets as bigint) out_octets",
    "try_cast(if_speed_mbps as int) if_speed_mbps",
    "admin_status",
    "try_cast(sys_uptime_s as bigint) sys_uptime_s",
    "poll_id", "_ingested_at")

print("unparseable values (NULL after try_cast):")
display(typed.select(
    F.sum(F.col("poll_ts").isNull().cast("int")).alias("bad_ts"),
    F.sum(F.col("in_octets").isNull().cast("int")).alias("bad_in"),
    F.sum(F.col("out_octets").isNull().cast("int")).alias("bad_out")))

# COMMAND ----------
# DECISION 1 — dedupe on poll_id, keep rank 1, BEFORE any LAG
w = Window.partitionBy("poll_id").orderBy("_ingested_at")
dd = typed.withColumn("rk", F.row_number().over(w)).filter("rk = 1").drop("rk", "_ingested_at")
dd.write.mode("overwrite").saveAsTable(T("silver_poll"))

n_in, n_out = bp.count(), spark.table(T("silver_poll")).count()
print(f"bronze {n_in:,} - duplicates {n_in - n_out:,} = silver {n_out:,}")
print("check 40 x 288 =", 40 * 288)
assert (n_in, n_out) == (13520, 11520) and n_out == 40 * 288

# COMMAND ----------
# DECISION 4 — dimension: TRIM the join keys (RTR-05 has a trailing space)
dim = bm.selectExpr("trim(device_id) device_id", "trim(interface_id) interface_id",
                    "site", "try_cast(rated_mbps as int) rated_mbps", "role")
dim.write.mode("overwrite").saveAsTable(T("dim_interface"))
d = spark.table(T("dim_interface"))
n_pairs = d.select("device_id", "interface_id").distinct().count()
print("dim rows:", d.count(), "| distinct trimmed pairs:", n_pairs)
assert d.count() == 40 and n_pairs == 40

# COMMAND ----------
# Evidence for decisions 2 and 3 (counted on the raw LAG, before correcting)
s = spark.table(T("silver_poll"))
w2 = Window.partitionBy("device_id", "interface_id").orderBy("poll_ts")
chk = (s.withColumn("d_in",  F.col("in_octets")  - F.lag("in_octets").over(w2))
        .withColumn("d_out", F.col("out_octets") - F.lag("out_octets").over(w2))
        .withColumn("d_up",  F.col("sys_uptime_s") - F.lag("sys_uptime_s").over(w2)))
r = chk.select(
    F.sum((F.col("d_in") < 0).cast("int")).alias("neg_in"),
    F.sum(((F.col("d_in") < 0) & (F.col("d_up") >= 0)).cast("int")).alias("in_wraps"),
    F.sum((F.col("d_up") < 0).cast("int")).alias("reboots")).first()
print(r)   # expect neg_in 1008, in_wraps 992, reboots 16
assert (r.neg_in, r.in_wraps, r.reboots) == (1008, 992, 16)
