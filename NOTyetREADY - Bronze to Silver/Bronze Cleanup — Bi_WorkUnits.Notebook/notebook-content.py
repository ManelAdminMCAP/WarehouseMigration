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

# # Bronze → Copper Transform — Bi_WorkUnits
# 
# **Purpose**: Read from Bronze, apply cleaning rules, and write cleaned data to Copper.
# 
# **Transformation Steps**:
# 1. **Filter deleted rows** using integration tables (`vEngagementsLastUpdate`, `vEngagementsMonthlyDataLastUpda`)
#    - Remove work units for `EngagementID` where `IsDeleted=1`
#    - Remove work units for `MonthlyDataID` where `IsDeleted=1`
# 2. **Deduplicate** keeping only the most recent version per composite PK `(ID, MonthlyDataID)`
# 3. **Write to copper** using Delta MERGE (upsert pattern)
# 
# **Processing Modes** (from Variable Library):
# - **`today`**: Transform only today's Bronze partition
# - **`all`**: Transform all Bronze partitions (full refresh)
# - **`last_n`**: Transform the last N partitions (catch-up/backfill)
# 
# **Tables**:
# | Table | Role |
# |---|---|
# | `bronze.Bi_WorkUnits` | Source fact table, partitioned by `ingestion_date` |
# | `bronze.vEngagementsLastUpdate` | Engagement-level delete flags |
# | `bronze.vEngagementsMonthlyDataLastUpdate` | MonthlyData-level delete signals |
# | `copper.Bi_WorkUnits` | Target cleaned table (deduplicated, no deleted records) |


# CELL ********************

# Cell 2 — Configuration & Spark setup
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timezone, timedelta

spark = SparkSession.builder.getOrCreate()

# Performance settings for large-scale operations
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
#spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
#spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# ---------------------------------------------------------------------------
# TABLE NAMES
# ---------------------------------------------------------------------------
BRONZE_WORK_UNITS    = "bronze.Bi_WorkUnits"
BRONZE_ENG_DELETES   = "bronze.vEngagementsLastUpdate"
BRONZE_MONTHLY       = "bronze.vEngagementsMonthlyDataLastUpdate"
COPPER_WORK_UNITS    = "copper.Bi_WorkUnits"  # Target table

# Primary key & timestamp for Bi_WorkUnits
PK_COLS      = ["ID", "MonthlyDataID"]
TIMESTAMP_COL = "LastUpdateDate"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Processing mode: "today" | "all" | "last_n"
PROCESSING_MODE= "last_n"

# Number of partitions to process when mode = "last_n"
LAST_N_PARTITIONS=1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"  Processing mode: {PROCESSING_MODE}")
if PROCESSING_MODE == "last_n":
    print(f"  Last N partitions: {LAST_N_PARTITIONS}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Determine Processing Scope
# 
# The notebook supports **three processing modes** (configured via Variable Library):
# 
# | Mode | Description | Use Case |
# |------|-------------|----------|
# | **`today`** | Process only today's partition | Daily scheduled cleanup after incremental load |
# | **`all`** | Process all partitions in Bronze | Initial cleanup or full reprocessing |
# | **`last_n`** | Process the last N partitions (most recent) | Catch-up after pipeline failures, backfill |
# 
# **Variable Library Configuration** (`V_bronze_config`):
# - `bronze_cleanup_mode`: Set to `"today"`, `"all"`, or `"last_n"`
# - `bronze_cleanup_last_n_partitions`: Number of partitions to process when mode = `"last_n"` (default: 7)
# 
# This avoids scanning all 180M+ rows by leveraging partition pruning — only specified `ingestion_date` partitions are processed.

# CELL ********************

# Resolve ingestion date(s) to process based on mode
print(f"Processing mode: {PROCESSING_MODE}")
print(f"{'='*70}\n")

# Get all distinct ingestion_date values (FAST - filesystem listing)
print("Discovering ingestion_date partitions from Bronze...")

# Method: List partition folders directly from filesystem (instant!)
# For partitioned tables, Fabric stores data in folders like:
#   Tables/Bi_WorkUnits/ingestion_date=2026-04-15/
# We can list these directories without reading any data files
try:
    # Get table location from catalog
    table_info = spark.sql(f"DESCRIBE DETAIL {BRONZE_WORK_UNITS}").collect()[0]
    table_location = table_info.location
    
    print(f"Table location: {table_location}")
    
    # List partition directories
    partition_folders = notebookutils.fs.ls(table_location)
    
    # Extract dates from folder names like "ingestion_date=2026-04-15"
    # Parse strings into Python date objects for proper type consistency
    from datetime import date
    
    all_partition_dates = []
    for folder in partition_folders:
        if folder.name.startswith("ingestion_date="):
            # Extract the date part after "ingestion_date="
            date_str = folder.name.replace("ingestion_date=", "").rstrip("/")
            # Parse string "2026-04-20" into date object date(2026, 4, 20)
            date_obj = date.fromisoformat(date_str)
            all_partition_dates.append(date_obj)
    
    # Sort in descending order (most recent first)
    all_partition_dates.sort(reverse=True)
    
    print(f"Total partitions found: {len(all_partition_dates)} (from filesystem)")
    if all_partition_dates:
        print(f"Sample dates: {all_partition_dates[:5]}")

except Exception as e:
    # Fallback: if filesystem listing fails, use Spark query
    print(f"⚠️ Filesystem listing failed, using Spark query: {e}")
    df_all_partitions = (
        spark.table(BRONZE_WORK_UNITS)
        .select("ingestion_date")
        .distinct()
        .orderBy(F.col("ingestion_date").desc())
    )
    # Spark returns date objects directly from DATE columns
    all_partition_dates = [
        row.ingestion_date for row in df_all_partitions.collect()
    ]
    print(f"Total partitions found: {len(all_partition_dates)} (from data scan)")

# Determine which partitions to process based on mode
if PROCESSING_MODE == "today":
    from datetime import date
    today_date = datetime.now(timezone.utc).date()
    target_dates = [today_date] if today_date in all_partition_dates else []
    print(f"Mode: TODAY → {today_date}")
    
elif PROCESSING_MODE == "all":
    # Process all partitions
    target_dates = all_partition_dates
    print(f"Mode: ALL → processing {len(target_dates)} partition(s)")
    
elif PROCESSING_MODE == "last_n":
    # Process the last N partitions (most recent)
    target_dates = all_partition_dates[:LAST_N_PARTITIONS]
    print(f"Mode: LAST_N → processing last {LAST_N_PARTITIONS} partition(s)")
    print(f"  Range: {target_dates[-1] if target_dates else 'N/A'} to {target_dates[0] if target_dates else 'N/A'}")
    
else:

    raise ValueError(f"Invalid PROCESSING_MODE: {PROCESSING_MODE}. Must be 'today', 'all', or 'last_n'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate: do we have any partitions to process?
if len(target_dates) == 0:
    print(f"\n⚠ No partitions found to process for mode '{PROCESSING_MODE}'")
    print("   This may happen if today's partition doesn't exist yet.")
else:
    print(f"\n✓ Partitions to process: {len(target_dates)}")
    df_BRONZE_WORK_UNITS = spark.table(BRONZE_WORK_UNITS)\
                                .filter(F.col("ingestion_date").isin(target_dates))

    
    # Show first 3 partitions
    for i, date in enumerate(target_dates[:3], 1):
        # Quick count for each partition (partition-pruned, fast)
        count = (
            spark.table(BRONZE_WORK_UNITS)
            .filter(F.col("ingestion_date") == date)
            .count()
        )
        print(f"  {i}. {date} → {count:,} rows")
    
    if len(target_dates) > 3:
        print(f"  ... and {len(target_dates) - 3} more partition(s)")
    
    # Total rows across all target partitions
    total_rows = (
        df_BRONZE_WORK_UNITS
        .count()
    )
    print(f"\nTotal rows to process: {total_rows:,}")
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Read Bronze Data (Apply Delete Filters)
# 
# Read Bronze data and **filter out deleted rows** using anti-joins. This avoids modifying Bronze — we simply exclude deleted records from the transformation.
# 
# **Strategy**:
# - Read only target partition(s) from Bronze (partition pruning)
# - LEFT ANTI JOIN to exclude rows with deleted `EngagementID`
# - LEFT ANTI JOIN to exclude rows with deleted `MonthlyDataID`
# - Result: clean DataFrame ready for deduplication

# CELL ********************

# Cell 6 — Read engagement-level deletes (from target partitions)
if len(target_dates) == 0:
    print("⚠ No partitions to process — skipping delete signal loading")
    df_eng_deletes = spark.createDataFrame([], "EngagementID: int")
    eng_delete_count = 0
else:
    df_eng_deletes = (
        spark.table(BRONZE_ENG_DELETES)
        .filter(F.col("ingestion_date").isin(target_dates))
        .filter(F.col("IsDeleted") == True)
        .select("EngagementID")
        .dropDuplicates()
    )

    eng_delete_count = df_eng_deletes.count()
    print(f"EngagementIDs to delete (across {len(target_dates)} partition(s)): {eng_delete_count:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 7 — Read MonthlyDataID-level deletes (from target partitions)
if len(target_dates) == 0:
    df_monthly_deletes = spark.createDataFrame([], "MonthlyDataID: int")
    monthly_delete_count = 0
else:
    df_monthly_deletes = (
        spark.table(BRONZE_MONTHLY)
        .filter(F.col("ingestion_date").isin(target_dates))
        .filter(F.col("IsDeleted") == True)
        .select("MonthlyDataID")
        .dropDuplicates()
    )

    monthly_delete_count = df_monthly_deletes.count()
    print(f"MonthlyDataIDs to delete (across {len(target_dates)} partition(s)): {monthly_delete_count:,}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Delete Flagged Rows from Bronze
# 
# Use Delta `MERGE ... WHEN MATCHED THEN DELETE` to remove rows **directly on the Delta table** without reading all 180M+ rows into a DataFrame.
# 
# Delta uses file-level metadata (min/max stats, Z-ORDER) to prune files — only files that *could* contain matching `EngagementID` / `MonthlyDataID` values are read and rewritten.
# 
# **Why MERGE instead of DELETE FROM ... WHERE IN?**  
# With `DELETE FROM ... WHERE EngagementID IN (subquery)`, Spark may broadcast the subquery or do a full scan depending on size. `MERGE` gives us explicit control and works well when the delete-key DataFrame is small.

# CELL ********************

# Cell 9 — Read Bronze and apply delete filters
print("Reading Bronze data with delete filters...")

if len(target_dates) == 0:
    print("⚠ No partitions to process — creating empty DataFrame")
    df_bronze_filtered = spark.createDataFrame([], spark.table(BRONZE_WORK_UNITS).schema)
else:
    # Read Bronze data for target partitions
    df_bronze = (
        spark.table(BRONZE_WORK_UNITS)
        .filter(F.col("ingestion_date").isin(target_dates))
    )
    
    bronze_count_before = df_bronze.count()
    print(f"Bronze rows (before filters): {bronze_count_before:,}")
    
    # Apply delete filter 1: Remove rows with deleted EngagementID
    if eng_delete_count > 0:
        df_bronze_filtered = (
            df_bronze
            .join(F.broadcast(df_eng_deletes), on="EngagementID", how="left_anti")
        )
        after_eng_filter = df_bronze_filtered.count()
        eng_filtered = bronze_count_before - after_eng_filter
        print(f"  ✓ Filtered {eng_filtered:,} rows by EngagementID")
    else:
        df_bronze_filtered = df_bronze
        print(f"  No EngagementID filters applied")
    
    # Apply delete filter 2: Remove rows with deleted MonthlyDataID
    if monthly_delete_count > 0:
        before_monthly = df_bronze_filtered.count()
        df_bronze_filtered = (
            df_bronze_filtered
            .join(F.broadcast(df_monthly_deletes), on="MonthlyDataID", how="left_anti")
        )
        after_monthly = df_bronze_filtered.count()
        monthly_filtered = before_monthly - after_monthly
        print(f"  ✓ Filtered {monthly_filtered:,} rows by MonthlyDataID")
    else:
        print(f"  No MonthlyDataID filters applied")
    
    final_count = df_bronze_filtered.count()
    print(f"\nBronze rows after delete filters: {final_count:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4 — Deduplicate (Keep Latest per PK)
# 
# Because Bronze uses **APPEND** mode, the same `(ID, MonthlyDataID)` can appear multiple times. We keep only the **most recent** version based on:
# - Latest `LastUpdateDate`
# 
# **Strategy**: Use window functions to rank rows per PK by `LastUpdateDate`, then filter to keep only rank=1 (winners).

# CELL ********************

# Deduplicate: keep only the latest row per (ID, MonthlyDataID)
# Memory-efficient approach: process in manageable chunks
print("Deduplicating Bronze data...")

if df_bronze_filtered.count() == 0:
    print("⚠ No data to deduplicate")
    df_clean = df_bronze_filtered
    dup_pk_count = 0
else:
    # Configuration for memory-efficient processing
    BATCH_SIZE_ROWS = 50_000_000  # Process 50M rows at a time
    
    # Estimate total rows
    total_rows = df_bronze_filtered.count()
    print(f"Total rows to deduplicate: {total_rows:,}")
    
    # Check for duplicates first (lightweight aggregation)
    print("\nChecking for duplicates...")
    df_pk_counts = (
        df_bronze_filtered
        .groupBy("ID", "MonthlyDataID")
        .agg(F.count("*").alias("row_count"))
        .filter(F.col("row_count") > 1)
    )
    
    dup_pk_count = df_pk_counts.count()
    print(f"PKs with duplicates: {dup_pk_count:,}")
    
    if dup_pk_count > 0:
        total_dup_rows = df_pk_counts.agg(F.sum("row_count")).first()[0]
        rows_to_remove = total_dup_rows - dup_pk_count
        print(f"Total rows involved in duplicates: {total_dup_rows:,}")
        print(f"Rows that will be deduplicated: {rows_to_remove:,}")
    
    # Memory-conscious deduplication strategy
    if total_rows <= BATCH_SIZE_ROWS:
        # Small enough to process in one go
        print(f"\nProcessing all {total_rows:,} rows in single batch...")
        
        w = Window.partitionBy("ID", "MonthlyDataID").orderBy(
            F.col("LastUpdateDate").desc()
        )
        
        df_clean = (
            df_bronze_filtered
            .withColumn("_rank", F.row_number().over(w))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
        )
    else:
        # Large dataset: use more efficient approach
        print(f"\n⚠️ Large dataset ({total_rows:,} rows) - using optimized deduplication...")
        print("Strategy: Aggregate to find latest timestamp per PK, then join back")
        
        # Step 1: Find the latest LastUpdateDate for each PK (lightweight aggregation)
        print("  [1/2] Finding latest timestamp per PK...")
        df_latest = (
            df_bronze_filtered
            .groupBy("ID", "MonthlyDataID")
            .agg(F.max("LastUpdateDate").alias("max_LastUpdateDate"))
        )
        
        # Step 2: Join to get only rows matching the latest timestamp
        print("  [2/2] Filtering to latest rows...")
        df_clean = (
            df_bronze_filtered
            .join(
                df_latest,
                on=[
                    (df_bronze_filtered.ID == df_latest.ID) &
                    (df_bronze_filtered.MonthlyDataID == df_latest.MonthlyDataID) &
                    (df_bronze_filtered.LastUpdateDate == df_latest.max_LastUpdateDate)
                ],
                how="inner"
            )
            .select(df_bronze_filtered["*"])  # Keep only original columns
        )
    
    clean_count = df_clean.count()
    print(f"\n✓ Deduplicated data: {clean_count:,} unique rows")
    
    # Cache for Silver write (only if dataset is manageable)
    if clean_count <= BATCH_SIZE_ROWS:
        df_clean.cache()
        df_clean.count()  # materialize
        print("✓ Clean data cached for Silver write")
    else:
        print("⚠️ Dataset too large to cache - will write directly to Silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5 — Write to Copper (Delta MERGE)
# 
# Write the cleaned and deduplicated data to `copper.Bi_WorkUnits` using Delta MERGE:
# - **WHEN MATCHED**: Update existing rows with newer data
# - **WHEN NOT MATCHED**: Insert new rows
# 
# This creates an idempotent, incremental upsert pattern safe for retry.

# CELL ********************

# Ensure copper schema exists
copper_schema = "copper"

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {copper_schema}")
    print(f"✓ Schema '{copper_schema}' is ready")
except Exception as e:
    print(f"⚠ Schema check: {e}")
    # Schema might already exist - continue

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write cleaned data to copper using MERGE
print(f"\nWriting to copper: {COPPER_WORK_UNITS}")

if df_clean.count() == 0:
    print("⚠ No data to write to copper")
else:
    # Check if copper table exists
    if not spark.catalog.tableExists(COPPER_WORK_UNITS):
        print(f"Creating copper table: {COPPER_WORK_UNITS}")
        # Initial load: write directly
        (
            df_clean
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(COPPER_WORK_UNITS)
        )
        rows_written = df_clean.count()
        print(f"✓ Created Copper table with {rows_written:,} rows")
    else:
        print("Merging into existing Copper table...")
        
        # Get Delta table handle
        copper_delta = DeltaTable.forName(spark, COPPER_WORK_UNITS)
        
        # MERGE: upsert based on composite PK (ID, MonthlyDataID)
        merge_condition = "target.ID = source.ID AND target.MonthlyDataID = source.MonthlyDataID"
        
        merge_result = (
            copper_delta.alias("target")
            .merge(
                df_clean.alias("source"),
                merge_condition
            )
            .whenMatchedUpdateAll()  # Update all columns when PK matches
            .whenNotMatchedInsertAll()  # Insert new PKs
            .execute()
        )
        
        print(f"✓ MERGE complete")
        
        # Show merge metrics (if available)
        try:
            print(f"  Rows inserted: {merge_result.get('num_inserted_rows', 'N/A')}")
            print(f"  Rows updated:  {merge_result.get('num_updated_rows', 'N/A')}")
        except:
            pass
    
    # Unpersist cached DataFrame
    df_clean.unpersist()
    
    # Final copper count
    copper_count = spark.table(COPPER_WORK_UNITS).count()
    print(f"\ncopper table total rows: {copper_count:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6 — Validation
# 
# Quick sanity checks after copper write:
# - No remaining duplicates in copper
# - No rows with deleted EngagementIDs in copper
# - No rows with deleted MonthlyDataIDs in copper

# CELL ********************

# Validate copper table
if not spark.catalog.tableExists(COPPER_WORK_UNITS):
    print("⚠ Copper table does not exist — skipping validation")
else:
    print("Validating Copper table...")
    
    # Validate: total count
    copper_total = spark.table(COPPER_WORK_UNITS).count()
    print(f"Total copper.Bi_WorkUnits rows: {copper_total:,}")
    
    # Validate: no remaining duplicates
    remaining_dups = (
        spark.table(COPPER_WORK_UNITS)
        .groupBy("ID", "MonthlyDataID")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    print(f"Duplicate PKs in Copper: {remaining_dups:,}")
    if remaining_dups > 0:
        print("⚠ WARNING: Duplicates still exist in Copper!")
    else:
        print("✓ No duplicates in Copper")
    
    # Validate: no deleted EngagementIDs remain
    if eng_delete_count > 0:
        leaked_eng = (
            spark.table(COPPER_WORK_UNITS)
            .join(F.broadcast(df_eng_deletes), on="EngagementID", how="inner")
            .count()
        )
        print(f"Leaked deleted EngagementID rows: {leaked_eng:,}")
        if leaked_eng > 0:
            print(f"⚠ WARNING: {leaked_eng} rows with deleted EngagementIDs in Copper!")
        else:
            print("✓ No deleted EngagementIDs in Copper")
    
    # Validate: no deleted MonthlyDataIDs remain
    if monthly_delete_count > 0:
        leaked_monthly = (
            spark.table(COPPER_WORK_UNITS)
            .join(F.broadcast(df_monthly_deletes), on="MonthlyDataID", how="inner")
            .count()
        )
        print(f"Leaked deleted MonthlyDataID rows: {leaked_monthly:,}")
        if leaked_monthly > 0:
            print(f"⚠ WARNING: {leaked_monthly} rows with deleted MonthlyDataIDs in Copper!")
        else:
            print("✓ No deleted MonthlyDataIDs in Copper")
    
    print("\n✓ Validation complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 7 — Summary & Audit Trail

# CELL ********************

# Transformation summary
print(f"{'='*70}")
print(f"  BRONZE → COPPER TRANSFORMATION SUMMARY")
print(f"  Processing mode:       {PROCESSING_MODE}")
if PROCESSING_MODE == "today":
    print(f"  Target partition:      {target_dates[0] if target_dates else 'N/A'}")
elif PROCESSING_MODE == "all":
    print(f"  Partitions processed:  {len(target_dates)}")
elif PROCESSING_MODE == "last_n":
    print(f"  Last N partitions:     {LAST_N_PARTITIONS}")
    print(f"  Partitions processed:  {len(target_dates)}")
    if target_dates:
        print(f"  Date range:            {target_dates[-1]} to {target_dates[0]}")
print(f"  Engagement deletes:    {eng_delete_count:,}")
print(f"  MonthlyData deletes:   {monthly_delete_count:,}")
print(f"  Duplicate PKs found:   {dup_pk_count:,}")
if spark.catalog.tableExists(COPPER_WORK_UNITS):
    copper_final = spark.table(COPPER_WORK_UNITS).count()
    print(f"  Copper total rows:     {copper_final:,}")
print(f"{'='*70}")

# Show Copper Delta history
if spark.catalog.tableExists(COPPER_WORK_UNITS):
    print("\nCopper table history (last 10 operations):")
    display(
        spark.sql(f"DESCRIBE HISTORY {COPPER_WORK_UNITS}")
        .select("version", "timestamp", "operation", "operationMetrics")
        .orderBy(F.col("version").desc())
        .limit(10)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# 
# ## Architecture Notes
# 
# ### Bronze → Copper Pattern
# 
# **Bronze Layer** (`bronze.Bi_WorkUnits`):
# - Immutable source of truth
# - APPEND mode preserves all historical data
# - Partitioned by `ingestion_date`
# - Contains duplicates and deleted records
# 
# **Copper Layer** (`copper.Bi_WorkUnits`):
# - Cleaned, deduplicated, business-ready data
# - Delta MERGE for idempotent upserts
# - No partitioning needed (Delta handles file organization)
# - Optimized for analytics queries
# 
# ### Processing Flow
# 
# ```
# Bronze Partition(s)
#     ↓
# Load Delete Signals (Integration Tables)
#     ↓
# Filter Deleted Rows (LEFT ANTI JOIN)
#     ↓
# Deduplicate (Window Functions → Rank=1)
#     ↓
# MERGE into Copper (Upsert by PK)
#     ↓
# Copper Table (Clean Data)
# ```
# 
# ### Performance Characteristics
# 
# | Operation | Data Volume | Strategy |
# |---|---|---|
# | **Read Bronze** | Partition-pruned | Only reads target `ingestion_date` partitions |
# | **Delete Filter** | Small broadcast | Integration tables are < 100K rows → broadcast JOIN |
# | **Deduplicate** | Window function | Ranks all rows in-memory, filters to rank=1 |
# | **MERGE to Copper** | Incremental upsert | Delta uses file stats to prune non-matching files |
# 
# ### When to Run
# 
# - **Daily**: After Bronze incremental load completes (mode=`today`)
# - **Catch-up**: After pipeline failures or backfills (mode=`last_n`)
# - **Full Refresh**: For Copper rebuild or schema changes (mode=`all`)
# 
# ### Integration with Pipeline
# 
# ```
# Pipeline Orchestration:
# 1. Copy Activity → Bronze (SQL Server → Lakehouse)
# 2. This Notebook  → Bronze → Copper Transform
# 3. Analytics/BI  → Query Copper tables
# ```

