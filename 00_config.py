# Databricks notebook source
# Shared settings. Change MY_ID once, here, and nowhere else.
MY_ID   = "yourname"                      # <-- put your roll number / name (lowercase, no spaces)
CATALOG = "workspace"
SCHEMA  = f"{CATALOG}.capstone_{MY_ID}"
VOL     = f"/Volumes/{CATALOG}/capstone_{MY_ID}/raw"

def T(name):
    """Fully-qualified table name that carries your ID (workspace is shared)."""
    return f"{SCHEMA}.{name}_{MY_ID}"

print("MY_ID =", MY_ID); print("SCHEMA =", SCHEMA); print("VOL =", VOL)
