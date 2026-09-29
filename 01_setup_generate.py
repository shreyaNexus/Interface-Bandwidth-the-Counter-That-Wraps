# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Setup + generate the raw data (Topic 34)
# MAGIC Think of this notebook as *building the router and switching it on*. It creates the schema and Volume, then writes the two raw CSVs.

# COMMAND ----------
# MAGIC %run ./00_config

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.capstone_{MY_ID}.raw")
for folder in ["raw", "export"]:
    dbutils.fs.mkdirs(f"{VOL}/{folder}")

# COMMAND ----------
# GENERATOR — exactly the brief's code (seed 34, counts untouched).
# Only two value-neutral safety edits, needed because ANSI mode is on:
#   (a) element_at index is cast to INT     (b) rated_mbps is cast to BIGINT before x 1e6 x 90 (else INT overflow)
W = 4294967296
PT = "partition by device_id, interface_id"
UB = f"{PT} order by p rows between unbounded preceding and current row"

i = spark.range(40).selectExpr("cast(1 + id div 4 as int) dv",
"cast(pmod(id, 4) as int) ix",
"format_string('RTR-%02d', 1 + id div 4) device_id",
"format_string('ge-0/0/%d', pmod(id, 4) + 1) interface_id",
"format_string('SITE-%02d', 1 + id div 4) site",
"element_at(array(10, 10, 50, 100), cast(pmod(id, 4) as int) + 1) rated_mbps",
"element_at(array('access', 'access', 'edge', 'core'), cast(pmod(id, 4) as int) + 1) role")
i.selectExpr("if(dv = 5, concat(device_id, ' '), device_id) device_id", "interface_id",
"site", "rated_mbps", "role").write.mode('overwrite').options(header=True,
ignoreLeadingWhiteSpace=False, ignoreTrailingWhiteSpace=False).csv(f'{VOL}/raw/if_master')

p = i.crossJoin(spark.range(288).withColumnRenamed('id', 'p'))
p = p.selectExpr("*", "(dv = 8 and p between 96 and 119) dn",
"case dv when 3 then 150 when 6 then 60 when 7 then 200 when 9 then 250 end pr",
"cast(cast(rated_mbps as bigint) * 1000000 * if(ix = 0 and dv <= 6,"
" if(p between 121 and 216, 90, 25), if(p between 121 and 216, 40, 15))"
" / 800 * 300 as bigint) d0")
p = p.selectExpr("*", f"sum(if(dn, 0, d0)) over ({UB}) ci",
f"sum(if(dn, 0, d0) div 2) over ({UB}) co")
p = p.selectExpr("*", f"max(if(p = pr, ci, null)) over ({PT}) ri",
f"max(if(p = pr, co, null)) over ({PT}) ro", f"max(if(p = pr, d0, null)) over ({PT}) rd")
p = p.selectExpr("device_id", "interface_id",
"timestamp'2025-05-06 00:00:00' + make_dt_interval(0, 0, cast(5 * p as int)) poll_ts",
f"pmod(if(pr is not null and p >= pr, ci - ri + rd div 10, ci), {W}) in_octets",
f"pmod(if(pr is not null and p >= pr, co - ro + (rd div 2) div 10, co), {W})"
" out_octets",
"if(dn, 0, rated_mbps) if_speed_mbps", "if(dn, 'down', 'up') admin_status",
"if(pr is not null and p >= pr, 30 + 300 * (p - pr), 100000 + dv * 1000 + 300 * p)"
" sys_uptime_s", "format_string('PL%02d%d%03d', dv, ix, p) poll_id", "p")
(p.drop('p').union(p.filter('p < 50').drop('p')).write.mode('overwrite')
.options(header=True, ignoreLeadingWhiteSpace=False, ignoreTrailingWhiteSpace=False)
.csv(f'{VOL}/raw/if_polls'))

# COMMAND ----------
# Count the FILES you wrote, not the DataFrame you think you wrote.
rd = lambda f: spark.read.option("header", True).csv(f"{VOL}/raw/{f}").count()
n_polls, n_master = rd("if_polls"), rd("if_master")
print("if_polls :", n_polls, "(expect 13,520)")
print("if_master:", n_master, "(expect 40)")
assert n_polls == 13520 and n_master == 40
