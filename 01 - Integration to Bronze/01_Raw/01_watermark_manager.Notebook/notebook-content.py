# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "15d22794-c0c7-49de-be6b-6c6fd1eff8e2",
# META       "default_lakehouse_name": "bronze_Lightway",
# META       "default_lakehouse_workspace_id": "ad2054da-ea66-4fe3-8a4a-6fde3ede0e33",
# META       "known_lakehouses": [
# META         {
# META           "id": "15d22794-c0c7-49de-be6b-6c6fd1eff8e2"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook: 01_watermark_manager
# ============================================================================
# Watermark Management — Raw Layer (Fabric Lakehouse Delta Table)
#
# PURPOSE:
#   Manage watermark state in a Delta table inside the Bronze Lakehouse,
#   instead of storing watermarks in the source SQL Server.
#
# WHY WATERMARK IN BRONZE (Fabric Lakehouse)?
#   - No dependency on source database for pipeline state
#   - Watermark travels with the Lakehouse (workspace promotion)
#   - Easy to inspect, reset, and audit via Spark SQL
#   - Works across dev/prod via the variable library
#
# WATERMARK TABLE: raw_watermarks
#   Columns:
#     - table_name     (STRING)  — source table identifier
#     - watermark_value (STRING) — last extracted timestamp (ISO format)
#     - load_type       (STRING) — "initial" or "incremental"
#     - updated_at      (TIMESTAMP) — when the watermark was last set
#
# USAGE FROM PIPELINE:
#   1. Pipeline calls this notebook with parameters:
#        { "action": "get", "table_name": "Bi_WorkUnits" }
#      → Returns the current watermark (or "1900-01-01" if first run)
#
#   2. After Copy Activity succeeds, pipeline calls:
#        { "action": "set", "table_name": "Bi_WorkUnits",
#          "new_watermark": "2025-04-15T10:30:00" }
#      → Updates the watermark to the new value
#
# USAGE FROM NOTEBOOK (direct call):
#   %run 01_watermark_manager
#   wm = get_watermark("Bi_WorkUnits")
#   set_watermark("Bi_WorkUnits", "2025-04-15T10:30:00")
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta.tables import DeltaTable
from datetime import datetime, timezone

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
WATERMARK_TABLE = "raw_watermarks"
DEFAULT_WATERMARK = "1900-01-01T00:00:00"

# ---------------------------------------------------------------------------
# INITIALIZE WATERMARK TABLE (idempotent)
# ---------------------------------------------------------------------------
def init_watermark_table():
    """Create the raw_watermarks Delta table if it doesn't exist."""
    if not spark.catalog.tableExists(WATERMARK_TABLE):
        schema = StructType([
            StructField("table_name", StringType(), False),
            StructField("watermark_value", StringType(), False),
            StructField("load_type", StringType(), False),
            StructField("updated_at", TimestampType(), False),
        ])
        df_empty = spark.createDataFrame([], schema)
        df_empty.write.format("delta").mode("overwrite").saveAsTable(WATERMARK_TABLE)
        print(f"  Created watermark table: {WATERMARK_TABLE}")
    else:
        print(f"  Watermark table exists: {WATERMARK_TABLE}")


# ---------------------------------------------------------------------------
# GET WATERMARK
# ---------------------------------------------------------------------------
def get_watermark(table_name: str) -> dict:
    """
    Get the current watermark for a table.
    
    Returns dict:
      {
        "watermark_value": "2025-04-15T10:30:00",  # or DEFAULT if first run
        "load_type": "incremental",                  # or "initial" if first run
        "is_initial_load": False                     # True if no watermark exists
      }
    """
    init_watermark_table()

    df = (
        spark.table(WATERMARK_TABLE)
        .filter(F.col("table_name") == table_name)
    )

    if df.count() == 0:
        print(f"  No watermark for '{table_name}' — this is an INITIAL LOAD")
        return {
            "watermark_value": DEFAULT_WATERMARK,
            "load_type": "initial",
            "is_initial_load": True,
        }

    row = df.orderBy(F.col("updated_at").desc()).first()
    wm_value = row["watermark_value"]
    load_type = row["load_type"]
    print(f"  Watermark for '{table_name}': {wm_value} (last load: {load_type})")

    return {
        "watermark_value": wm_value,
        "load_type": "incremental",
        "is_initial_load": False,
    }


# ---------------------------------------------------------------------------
# SET WATERMARK
# ---------------------------------------------------------------------------
def set_watermark(table_name: str, new_watermark: str, load_type: str = "incremental"):
    """
    Upsert the watermark for a given table in the raw_watermarks Delta table.
    
    Args:
        table_name:    Source table name (e.g., "Bi_WorkUnits")
        new_watermark: New watermark value (ISO timestamp string)
        load_type:     "initial" or "incremental"
    """
    init_watermark_table()

    now = datetime.now(timezone.utc)

    df_new = spark.createDataFrame(
        [(table_name, new_watermark, load_type, now)],
        ["table_name", "watermark_value", "load_type", "updated_at"],
    )

    wm_delta = DeltaTable.forName(spark, WATERMARK_TABLE)

    (
        wm_delta.alias("target")
        .merge(
            df_new.alias("source"),
            "target.table_name = source.table_name"
        )
        .whenMatchedUpdate(set={
            "watermark_value": "source.watermark_value",
            "load_type": "source.load_type",
            "updated_at": "source.updated_at",
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"  Watermark set: {table_name} = {new_watermark} ({load_type})")


# ---------------------------------------------------------------------------
# RESET WATERMARK (for re-processing / testing)
# ---------------------------------------------------------------------------
def reset_watermark(table_name: str):
    """Reset watermark to force a full initial reload on next pipeline run."""
    init_watermark_table()

    if not spark.catalog.tableExists(WATERMARK_TABLE):
        print(f"  No watermark table — nothing to reset")
        return

    wm_delta = DeltaTable.forName(spark, WATERMARK_TABLE)
    wm_delta.delete(F.col("table_name") == table_name)
    print(f"  Watermark reset for '{table_name}' — next run will be initial load")


def reset_all_watermarks():
    """Reset ALL watermarks — forces full reload for every table."""
    init_watermark_table()
    wm_delta = DeltaTable.forName(spark, WATERMARK_TABLE)
    wm_delta.delete()
    print(f"  All watermarks reset")


# ---------------------------------------------------------------------------
# VIEW ALL WATERMARKS
# ---------------------------------------------------------------------------
def show_watermarks():
    """Display current state of all watermarks."""
    init_watermark_table()
    spark.table(WATERMARK_TABLE).orderBy("table_name").show(truncate=False)


# ---------------------------------------------------------------------------
# GET MAX TIMESTAMP FROM BRONZE TABLE (for post-load watermark update)
# ---------------------------------------------------------------------------
def get_max_timestamp_from_raw(raw_table: str, timestamp_col: str, ingestion_date: str = None) -> str:
    """
    Read the MAX timestamp from a Raw table for a given ingestion date.
    Used after Copy Activity to determine the new watermark value.
    
    Args:
        raw_table:   Name of the Raw Delta table
        timestamp_col:  Column to get MAX of (e.g., "LastUpdateDate")
        ingestion_date: Filter to specific ingestion partition (optional)
    
    Returns:
        ISO string of the max timestamp, or None if no data
    """
    df = spark.table(raw_table)

    if ingestion_date:
        df = df.filter(F.col("ingestion_date") == ingestion_date)

    result = df.agg(F.max(F.col(timestamp_col)).alias("max_ts")).first()

    if result["max_ts"] is None:
        print(f"  No data in {raw_table} — watermark unchanged")
        return None

    max_ts = str(result["max_ts"])
    print(f"  Max {timestamp_col} in {raw_table}: {max_ts}")
    return max_ts



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# ENTRY POINT (when called from pipeline with parameters)
# ============================================================================
# In Fabric Data Pipeline, call this notebook with parameters:
#
#   Action "get":
#     { "action": "get", "table_name": "Bi_WorkUnits" }
#     → Notebook exit value: watermark_value (used by next activity)
#
#   Action "set":
#     { "action": "set", "table_name": "Bi_WorkUnits",
#       "new_watermark": "2025-04-15T10:30:00", "load_type": "incremental" }
#
#   Action "show":
#     { "action": "show" }
# ============================================================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

action = "get"
p_table_name = "Bi_WorkUnits"
p_new_watermark = ""
p_load_type = "incremental"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if action == "get" and p_table_name:
    wm = get_watermark(p_table_name)
    # Return watermark to pipeline via notebook exit value
    try:
        mssparkutils.notebook.exit(wm["watermark_value"])
    except NameError:
        print(f"EXIT_VALUE: {wm['watermark_value']}")

elif action == "set" and p_table_name and p_new_watermark:
    set_watermark(p_table_name, p_new_watermark, p_load_type)

elif action == "reset" and p_table_name:
    reset_watermark(p_table_name)

elif action == "show":
    show_watermarks()

else:
    print("Watermark manager loaded. Available functions:")
    print("  get_watermark(table_name)")
    print("  set_watermark(table_name, new_watermark, load_type)")
    print("  reset_watermark(table_name)")
    print("  show_watermarks()")
    print("  get_max_timestamp_from_raw(raw_table, timestamp_col)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
