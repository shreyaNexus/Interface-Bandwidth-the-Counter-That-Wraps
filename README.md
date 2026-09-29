# Capstone — Topic 34: Interface Bandwidth & the Counter That Wraps

## Run order (Databricks)
1. Import the 6 `.py` files into one Workspace folder (Import → file; they open as notebooks).
2. Edit `MY_ID` in `00_config.py` (once).
3. Run `01` → `02` → `03` → `04` → `05` in order (serverless is fine).
4. Download the 3 CSVs from Catalog → your Volume → `export`.
5. Run `06_snowflake.sql` in Snowsight, uploading the CSVs to the stage when it says so.

## Databricks Job (schedule it — worth marks)
Workflows → Create Job → 5 tasks in a chain: `01_setup_generate → 02_bronze → 03_silver → 04_gold → 05_export`.
Add a schedule (e.g. daily), and let it fire once on its own. Screenshot the successful run.

## Numbers every step must show
| Step | Expect |
|---|---|
| Bronze polls / master | 13,520 / 40 |
| Silver polls | 11,520 (2,000 duplicate poll_ids removed; 40 x 288) |
| Negative in_octets deltas | 1,008 = 992 wraps + 16 reboots |
| Gold rows | 11,480; 16 NULL in_mbps; max delta <= 1,500,000,000 |
| Intervals above 80% | 575 = 2,875 min (96 each on 5 links, 95 on RTR-03 ge-0/0/1) |
| Wraps by role | core 55-58, edge 27-29, access 5-11 |

## Decisions log
1. Dedupe on `poll_id` before LAG (otherwise a row is compared with its own copy → delta 0).
2. Negative delta = counter wrap → add 2^32 and keep the row (never `WHERE delta > 0`).
3. sys_uptime_s fell = reboot → delta NULL, keep the row (never add 2^32).
4. TRIM the join key (RTR-05 trailing space) and use `NULLIF(rated_mbps, 0)`; speed comes from the master, not `if_speed_mbps`.
5. `try_cast`, never `cast` (ANSI mode); `in_octets` to BIGINT.
6. Generator edits: two value-neutral casts for ANSI mode (see comment in notebook 01).
