-- Topic 34 · Snowflake side (your own account: no ID prefix needed)
CREATE DATABASE IF NOT EXISTS CAPSTONE;
CREATE SCHEMA   IF NOT EXISTS CAPSTONE.NETWORK;
USE SCHEMA CAPSTONE.NETWORK;

CREATE OR REPLACE FILE FORMAT csv_fmt
  TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL') EMPTY_FIELD_AS_NULL = TRUE;

CREATE STAGE IF NOT EXISTS net_stage FILE_FORMAT = csv_fmt;
-- Upload the 3 CSVs: Snowsight > Data > Databases > CAPSTONE > NETWORK > Stages > NET_STAGE > + Files
-- (or SnowSQL:  PUT file:///path/gold_iface_interval.csv @net_stage;  same for the other two)
LIST @net_stage;

CREATE OR REPLACE TABLE GOLD_IFACE_INTERVAL (
  device_id STRING, interface_id STRING, site STRING, role STRING, rated_mbps INT,
  bucket_start TIMESTAMP_NTZ, in_octets_delta BIGINT, out_octets_delta BIGINT,
  in_mbps DOUBLE, out_mbps DOUBLE, util_pct DOUBLE, wrap_flag BOOLEAN, reboot_flag BOOLEAN);

CREATE OR REPLACE TABLE SILVER_POLL (
  device_id STRING, interface_id STRING, poll_ts TIMESTAMP_NTZ, in_octets BIGINT,
  out_octets BIGINT, if_speed_mbps INT, admin_status STRING, sys_uptime_s BIGINT, poll_id STRING);

CREATE OR REPLACE TABLE DIM_INTERFACE (
  device_id STRING, interface_id STRING, site STRING, rated_mbps INT, role STRING);

-- Run each COPY TWICE. The 2nd run must say 0 files loaded (load metadata skips the same file).
COPY INTO GOLD_IFACE_INTERVAL FROM @net_stage/gold_iface_interval.csv;
COPY INTO GOLD_IFACE_INTERVAL FROM @net_stage/gold_iface_interval.csv;   -- expect: 0 files
COPY INTO SILVER_POLL         FROM @net_stage/silver_poll.csv;
COPY INTO SILVER_POLL         FROM @net_stage/silver_poll.csv;           -- expect: 0 files
COPY INTO DIM_INTERFACE       FROM @net_stage/dim_interface.csv;
COPY INTO DIM_INTERFACE       FROM @net_stage/dim_interface.csv;         -- expect: 0 files

-- Row-count checks: 11480 / 11520 / 40
SELECT (SELECT COUNT(*) FROM GOLD_IFACE_INTERVAL) g, (SELECT COUNT(*) FROM SILVER_POLL) s, (SELECT COUNT(*) FROM DIM_INTERFACE) d;

-- Q1. Throughput in Mbps per interface per 5-minute bucket
SELECT device_id, interface_id, bucket_start, ROUND(in_mbps,3) in_mbps, ROUND(out_mbps,3) out_mbps, ROUND(util_pct,1) util_pct
FROM GOLD_IFACE_INTERVAL ORDER BY device_id, interface_id, bucket_start;

-- Q2. Interfaces above 80% of rated, and minutes per day  (expect 6 rows, 2,875 minutes total)
SELECT device_id, interface_id, rated_mbps, COUNT(*) * 5 AS minutes_above_80
FROM GOLD_IFACE_INTERVAL WHERE util_pct > 80
GROUP BY 1,2,3 ORDER BY minutes_above_80 DESC;

-- Q3 (the worked "hard one" from the brief): wraps vs reboots vs peak %, straight from SILVER_POLL  (expect 40 rows)
WITH d AS (SELECT device_id, interface_id, poll_ts,
    in_octets - LAG(in_octets) OVER (PARTITION BY device_id, interface_id ORDER BY poll_ts) AS raw_d,
    sys_uptime_s - LAG(sys_uptime_s) OVER (PARTITION BY device_id, interface_id ORDER BY poll_ts) AS up_d
  FROM SILVER_POLL),
c AS (SELECT d.*, CASE WHEN up_d < 0 THEN NULL
                       WHEN raw_d < 0 THEN raw_d + 4294967296 ELSE raw_d END AS octets FROM d)
SELECT c.device_id, c.interface_id, m.rated_mbps,
  COUNT_IF(c.raw_d < 0 AND c.up_d >= 0) AS wraps,
  COUNT_IF(c.up_d < 0) AS reboots,
  ROUND(MAX(c.octets * 8.0 / 300 / 1e6 / NULLIF(m.rated_mbps, 0)) * 100, 1) AS peak_pct
FROM c JOIN DIM_INTERFACE m
  ON TRIM(m.device_id) = c.device_id AND TRIM(m.interface_id) = c.interface_id
GROUP BY 1,2,3 ORDER BY wraps DESC, peak_pct DESC;
