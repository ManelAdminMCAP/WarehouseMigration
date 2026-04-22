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

# MARKDOWN ********************

# # Raw → Bronze Ingestion — Simple Append
# 
# **Purpose**: Fast ingestion from Raw to Bronze for tables loaded with append mode that **don't have an updateDate column**.
# 
# **Core Strategy**:
# - **Process latest partition(s) only** — Default: last partition from Raw (most recent data)
# - **Partition-based deduplication** — Keep row from latest `ingestion_date` per primary key
# - **Dynamic partition OVERWRITE** — 5-10x faster than MERGE, no CU overhead
# - **Minimal transformation** — Focus on speed and data quality
# 
# **Data Flow**:
# ```
# Raw (partitioned by ingestion_date)
#   ↓ Read latest partition(s)
#   ↓ Deduplicate using ingestion_date DESC
#   ↓ OVERWRITE to Bronze partition
# Bronze (deduplicated, partitioned by ingestion_date)
# ```
# 
# **Processing Modes** (configurable):
# - **`last_n`** (default): Process the last N partitions — **recommended for scheduled runs**
# - **`today`**: Process only today's partition
# - **`all`**: Full reprocess of all partitions (use sparingly)
# 
# **Use Cases**:
# - ✅ Scheduled incremental ingestion (daily/hourly)
# - ✅ Append-only source tables without update timestamps
# - ✅ Fast bulk loads where partition-level OVERWRITE is acceptable
# - ✅ Simple fact tables without complex change tracking


# CELL ********************

# Configuration & Spark setup
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timezone, date

spark = SparkSession.builder.getOrCreate()

# Performance settings
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# ---------------------------------------------------------------------------
# CONFIGURATION — UPDATE THESE VALUES
# ---------------------------------------------------------------------------
RAW_TABLE = "raw.vEngagementsLastUpdate"            # Source table (Raw layer)
BRONZE_TABLE = "bronze.vEngagementsLastUpdate"      # Target table (Bronze layer)

# Primary key columns (used for deduplication)
# Example: ["ID"] or ["CustomerID", "OrderID"]
PK_COLS = ["EngagementID"]

# Processing mode: "last_n" (recommended) | "today" | "all"
# - "last_n": Process the most recent N partitions (default, recommended)
# - "today": Process only today's partition
# - "all": Full reprocess of all partitions (use sparingly)
PROCESSING_MODE = "last_n"

# Number of partitions to process when mode = "last_n"
# Default: 1 (process only the latest partition for incremental loads)
LAST_N_PARTITIONS = 1

print(f"Raw Table: {RAW_TABLE}")
print(f"Bronze Table: {BRONZE_TABLE}")
print(f"Primary Key: {PK_COLS}")
print(f"Processing Mode: {PROCESSING_MODE}")
if PROCESSING_MODE == "last_n":
    print(f"Last N Partitions: {LAST_N_PARTITIONS}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Determine Processing Scope (Partition Discovery)
# 
# Identify which `ingestion_date` partitions to process based on the configured mode.
# 
# **Approach**: Fast filesystem-based partition discovery (no data scan required).

# CELL ********************

# Discover available partitions from Raw table
print(f"Discovering partitions from {RAW_TABLE}...")
print(f"{'='*70}\n")

try:
    # Method 1: List partition folders from filesystem (fast)
    table_info = spark.sql(f"DESCRIBE DETAIL {RAW_TABLE}").collect()[0]
    table_location = table_info.location
    
    print(f"Table location: {table_location}")
    
    # List partition directories
    partition_folders = notebookutils.fs.ls(table_location)
    
    # Extract dates from folder names like "ingestion_date=2026-04-15"
    all_partition_dates = []
    for folder in partition_folders:
        if folder.name.startswith("ingestion_date="):
            date_str = folder.name.replace("ingestion_date=", "").rstrip("/")
            date_obj = date.fromisoformat(date_str)
            all_partition_dates.append(date_obj)
    
    # Sort in descending order (most recent first)
    all_partition_dates.sort(reverse=True)
    
    print(f"Total partitions found: {len(all_partition_dates)}")
    if all_partition_dates:
        print(f"Sample dates: {all_partition_dates[:5]}")

except Exception as e:
    # Fallback: Use Spark query if filesystem listing fails
    print(f"⚠️ Filesystem listing failed, using Spark query: {e}")
    df_all_partitions = (
        spark.table(RAW_TABLE)
        .select("ingestion_date")
        .distinct()
        .orderBy(F.col("ingestion_date").desc())
    )
    all_partition_dates = [row.ingestion_date for row in df_all_partitions.collect()]
    print(f"Total partitions found: {len(all_partition_dates)}")

# Determine which partitions to process
if PROCESSING_MODE == "today":
    today_date = datetime.now(timezone.utc).date()
    target_dates = [today_date] if today_date in all_partition_dates else []
    print(f"\nMode: TODAY → {today_date}")
    
elif PROCESSING_MODE == "all":
    target_dates = all_partition_dates
    print(f"\nMode: ALL → processing {len(target_dates)} partition(s)")
    
elif PROCESSING_MODE == "last_n":
    target_dates = all_partition_dates[:LAST_N_PARTITIONS]
    print(f"\nMode: LAST_N → processing last {LAST_N_PARTITIONS} partition(s)")
    if target_dates:
        print(f"  Range: {target_dates[-1]} to {target_dates[0]}")
    
else:
    raise ValueError(f"Invalid PROCESSING_MODE: {PROCESSING_MODE}")

# Validate partitions exist
if len(target_dates) == 0:
    print(f"\n⚠️ No partitions found to process for mode '{PROCESSING_MODE}'")
    print("   Stopping execution.")
else:
    print(f"\n✓ Partitions to process: {len(target_dates)}")
    
    # Show sample partition counts
    for i, date_val in enumerate(target_dates[:3], 1):
        count = spark.table(RAW_TABLE).filter(F.col("ingestion_date") == date_val).count()
        print(f"  {i}. {date_val} → {count:,} rows")
    
    if len(target_dates) > 3:
        print(f"  ... and {len(target_dates) - 3} more partition(s)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Read and Deduplicate Using Partition-Based Strategy
# 
# Deduplication strategy using partition information:
# - Keep the row from the **latest partition** (most recent `ingestion_date`) for each primary key
# - This leverages the fact that Raw is partitioned by `ingestion_date`
# - More recent partitions contain the latest data
# 
# **Why use ingestion_date for deduplication?**
# - ✅ Logical: Latest partition = most current data
# - ✅ Efficient: Uses existing partition column (no synthetic ordering needed)
# - ✅ Deterministic: Clear, reproducible results
# 
# **Note**: If your source system guarantees no duplicates, you can skip this step entirely.

# CELL ********************

# Read Raw data for target partitions
if len(target_dates) == 0:
    print("⚠️ No partitions to process")
    df_clean = spark.createDataFrame([], spark.table(RAW_TABLE).schema)
else:
    print(f"Reading Raw data from {len(target_dates)} partition(s)...")
    
    df_raw = (
        spark.table(RAW_TABLE)
        .filter(F.col("ingestion_date").isin(target_dates))
    )
    
    total_rows = df_raw.count()
    print(f"Total rows read: {total_rows:,}")
    
    # Check for duplicates
    print("\nChecking for duplicate primary keys...")
    df_duplicates = (
        df_raw
        .groupBy(*PK_COLS)
        .agg(F.count("*").alias("row_count"))
        .filter(F.col("row_count") > 1)
    )
    
    dup_count = df_duplicates.count()
    
    if dup_count == 0:
        print("✓ No duplicates found — all primary keys are unique")
        df_clean = df_raw
    else:
        print(f"⚠️ Found {dup_count:,} duplicate primary keys")
        
        # Deduplicate: Keep row from the LATEST partition (most recent ingestion_date)
        # This ensures we keep the most current data for each primary key
        window_spec = Window.partitionBy(*PK_COLS).orderBy(F.col("ingestion_date").desc())
        
        df_clean = (
            df_raw
            .withColumn("_row_num", F.row_number().over(window_spec))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Write to Bronze Using Dynamic Partition OVERWRITE
# 
# Write the cleaned and deduplicated data to Bronze using **OVERWRITE** mode for maximum performance.
# 
# **Strategy**:
# - **Dynamic partition OVERWRITE** — only specified partitions are replaced
# - Partition-level atomicity ensures data consistency
# - Idempotent: safe to re-run for same partition
# - ⚠️ Replaces entire partition (not row-level updates)

# CELL ********************

# Write to Bronze table using OVERWRITE mode
if len(target_dates) == 0 or df_clean.count() == 0:
    print("⚠️ No data to write to Bronze")
else:
    print(f"Writing to {BRONZE_TABLE} using OVERWRITE mode...")
    print(f"Target partitions: {target_dates}")
    
    # Write using partition overwrite
    # This overwrites ONLY the specified partitions, not the entire table
    df_clean.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "false") \
        .option("partitionOverwriteMode", "dynamic") \
        .partitionBy("ingestion_date") \
        .saveAsTable(BRONZE_TABLE)
    
    print("✓ Write complete")
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Notes
# 
# **When to use this notebook**:
# - ✅ **Scheduled incremental ingestion** (daily/hourly) processing latest partition(s)
# - ✅ Raw → Bronze ingestion for append-only tables
# - ✅ Tables without update timestamps or change tracking
# - ✅ Fast bulk loads where partition-level OVERWRITE is acceptable
# 
# **When NOT to use this notebook**:
# - ❌ Tables with `LastUpdateDate` requiring timestamp-based deduplication
# - ❌ Tables with delete signals from integration tables
# - ❌ Cases requiring row-level updates (use MERGE-based approach instead)
# 
# **Customization**:
# 1. Update `RAW_TABLE` and `BRONZE_TABLE` names
# 2. Set `PK_COLS` to match your primary key
# 3. Keep `PROCESSING_MODE = "last_n"` with `LAST_N_PARTITIONS = 1` for daily scheduled runs (recommended)
# 4. Increase `LAST_N_PARTITIONS` for catch-up scenarios (e.g., 7 for a week of backfill)
# 5. Deduplication uses `ingestion_date` (latest partition wins) — modify if different logic needed
# 
# **Performance Tips**:
# - **Default `last_n=1` mode** processes only the latest partition (optimal for scheduled incremental loads)
# - Dynamic partition OVERWRITE is 5-10x faster than MERGE for bulk ingestion
# - Run OPTIMIZE on Bronze table periodically to compact small files
# - Enable Auto Optimize for production workloads
# - Avoid `all` mode in production — use only for full reprocessing scenarios

