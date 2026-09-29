# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze — land it, change nothing
# MAGIC Like photocopying a document before anyone writes on it. Every column stays a string; no trim, no dedupe, no sort, no join.

# COMMAND ----------
# MAGIC %run ./00_config

# COMMAND ----------
import pyspark.sql.functions as F

def bronze(table, folder):
    df = (spark.read.option("header", True).option("inferSchema", False)
          .option("ignoreLeadingWhiteSpace", False).option("ignoreTrailingWhiteSpace", False)
          .csv(f"{VOL}/raw/{folder}"))
    raw_cols = df.columns
    df = (df.withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_row_hash", F.sha2(F.concat_ws("||",
                  *[F.coalesce(F.col(c), F.lit("<null>")) for c in raw_cols]), 256)))
    df.write.mode("overwrite").saveAsTable(T(table))
    return spark.table(T(table)).count()

n_polls  = bronze("bronze_polls",  "if_polls")
n_master = bronze("bronze_master", "if_master")
print("bronze polls :", n_polls,  "(expect 13,520)")
print("bronze master:", n_master, "(expect 40)")
assert n_polls == 13520 and n_master == 40

# COMMAND ----------
# The trailing space in RTR-05 must still be visible in Bronze.
display(spark.sql(f"SELECT device_id, length(device_id) len FROM {T('bronze_master')} WHERE length(device_id) <> length(trim(device_id))"))
