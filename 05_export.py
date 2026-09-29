# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Export — three tables, three single CSVs
# MAGIC Gold + the two tables the Snowflake "hard one" query reads (SILVER_POLL, DIM_INTERFACE). Small enough for pandas → exactly one file each.

# COMMAND ----------
# MAGIC %run ./00_config

# COMMAND ----------
for tbl, fname in [("gold_iface_interval", "gold_iface_interval.csv"),
                   ("silver_poll",         "silver_poll.csv"),
                   ("dim_interface",       "dim_interface.csv")]:
    pdf = spark.table(T(tbl)).toPandas()
    if "poll_ts" in pdf: pdf = pdf.sort_values(["device_id", "interface_id", "poll_ts"])
    pdf.to_csv(f"{VOL}/export/{fname}", index=False)
    print(f"{fname}: {len(pdf):,} rows")

display(dbutils.fs.ls(f"{VOL}/export"))
# Download these 3 files from Catalog > your Volume > export, then upload to the Snowflake stage.
