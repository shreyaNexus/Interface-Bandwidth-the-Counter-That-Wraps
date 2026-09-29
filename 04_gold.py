# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold — one row per interface per 5-minute interval
# MAGIC Odometer rule: **negative = rolled over → add 2^32.** Unless the uptime also fell — then the car was *replaced*, so the distance is unknown → NULL.

# COMMAND ----------
# MAGIC %run ./00_config

# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql import Window
WRAP = 4294967296

sp  = spark.table(T("silver_poll"))
dim = spark.table(T("dim_interface"))

w = Window.partitionBy("device_id", "interface_id").orderBy("poll_ts")
d = (sp.withColumn("prev_ts", F.lag("poll_ts").over(w))
       .withColumn("raw_in",  F.col("in_octets")  - F.lag("in_octets").over(w))
       .withColumn("raw_out", F.col("out_octets") - F.lag("out_octets").over(w))
       .withColumn("up_d",    F.col("sys_uptime_s") - F.lag("sys_uptime_s").over(w))
       .filter("prev_ts is not null"))            # first poll has no predecessor -> no row

d = (d.withColumn("reboot_flag", F.col("up_d") < 0)
      .withColumn("wrap_flag", (F.col("raw_in") < 0) & ~F.col("reboot_flag")))

def fix(c):
    return (F.when(F.col("reboot_flag"), F.lit(None).cast("bigint"))     # reboot -> unknown
             .when(F.col(c) < 0, F.col(c) + F.lit(WRAP))                 # wrap   -> add 2^32
             .otherwise(F.col(c)))

d = d.withColumn("in_octets_delta", fix("raw_in")).withColumn("out_octets_delta", fix("raw_out"))

g = (d.join(dim, ["device_id", "interface_id"], "left")     # keys already trimmed in dim
      .withColumn("bucket_start", F.col("prev_ts"))
      .withColumn("in_mbps",  F.col("in_octets_delta")  * 8 / 300 / 1e6)
      .withColumn("out_mbps", F.col("out_octets_delta") * 8 / 300 / 1e6)
      .withColumn("util_pct", F.col("in_mbps") / F.nullif(F.col("rated_mbps"), F.lit(0)) * 100)
      .select("device_id", "interface_id", "site", "role", "rated_mbps", "bucket_start",
              "in_octets_delta", "out_octets_delta", "in_mbps", "out_mbps", "util_pct",
              "wrap_flag", "reboot_flag"))
g.write.mode("overwrite").saveAsTable(T("gold_iface_interval"))

# COMMAND ----------
# CHECKS from the brief
gt = spark.table(T("gold_iface_interval"))
row = gt.select(
    F.count("*").alias("rows"),
    F.sum(F.col("wrap_flag").cast("int")).alias("wraps"),
    F.sum(F.col("reboot_flag").cast("int")).alias("reboots"),
    F.sum(F.col("in_mbps").isNull().cast("int")).alias("null_in_mbps"),
    F.max("in_octets_delta").alias("max_delta"),
    F.sum((F.col("util_pct") > 100).cast("int")).alias("over_100pct"),
    F.sum((F.col("util_pct") > 80).cast("int")).alias("intervals_over_80")).first()
print(row)
assert row.rows == 11480 and row.wraps == 992 and row.reboots == 16 and row.null_in_mbps == 16
assert row.max_delta <= 1_500_000_000 and row.over_100pct == 0
assert row.intervals_over_80 == 575
print("minutes above 80% =", row.intervals_over_80 * 5, "(expect 2,875)")

# COMMAND ----------
# Question 2: which interfaces run >80% of rated, and for how many minutes/day?
display(spark.sql(f"""
  SELECT device_id, interface_id, rated_mbps, COUNT(*) * 5 AS minutes_above_80
  FROM {T('gold_iface_interval')} WHERE util_pct > 80
  GROUP BY 1,2,3 ORDER BY minutes_above_80 DESC"""))   # 5 links x 480 + RTR-03 x 475

# Question 3: wraps by role — core (100 Mbps) leads, not the 90%-loaded links
display(spark.sql(f"""
  SELECT device_id, interface_id, rated_mbps, SUM(CAST(wrap_flag AS INT)) wraps
  FROM {T('gold_iface_interval')} GROUP BY 1,2,3 ORDER BY wraps DESC"""))
