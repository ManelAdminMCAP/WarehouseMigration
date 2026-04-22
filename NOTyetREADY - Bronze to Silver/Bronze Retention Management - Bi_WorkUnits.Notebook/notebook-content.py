# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "warehouse": {
# META       "default_warehouse": "a47c01ee-2d9b-4eb8-ada5-f3ff816ca834",
# META       "known_warehouses": [
# META         {
# META           "id": "a47c01ee-2d9b-4eb8-ada5-f3ff816ca834",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Bronze Retention Management (Optional)
# 
# **Problem**: Bronze uses APPEND mode → data accumulates indefinitely → can grow to TB without cleanup.
# 
# **Solution**: Drop old `ingestion_date` partitions after a retention period (e.g., 90 days).
# 
# **Why this is safe**:
# - Silver already has the cleaned, deduplicated data
# - Bronze is only needed for reprocessing/audit trail
# - Partition-level DELETE is instant (just drops directories)
# 
# **When to run**: Weekly or monthly, after Silver is confirmed up-to-date.

# CELL ********************

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timezone, timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BRONZE_WORK_UNITS  = "bronze.Bi_WorkUnits"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Bronze retention policy
# Set retention period (days to keep in Bronze)
BRONZE_RETENTION_DAYS = 30  # Adjust based on your reprocessing/audit needs

# Enable/disable retention cleanup
RUN_RETENTION_CLEANUP = True  # Set to True to clean old partitions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if RUN_RETENTION_CLEANUP:
    print(f"Bronze Retention Policy: Keep last {BRONZE_RETENTION_DAYS} days")
    print(f"{'='*70}\n")
    
    # Calculate cutoff date
    from datetime import timedelta
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=BRONZE_RETENTION_DAYS)).strftime("%Y-%m-%d")
    print(f"Cutoff date: {cutoff_date}")
    print(f"Partitions BEFORE this date will be deleted.\n")
    
    # Get all partitions older than retention period
    df_old_partitions = (
        spark.table(BRONZE_WORK_UNITS)
        .select("ingestion_date")
        .distinct()
        .filter(F.col("ingestion_date") < cutoff_date)
        .orderBy("ingestion_date")
    )
    
    old_partitions = [row.ingestion_date for row in df_old_partitions.collect()]
    
    if len(old_partitions) == 0:
        print("✓ No old partitions to delete")
    else:
        print(f"Found {len(old_partitions)} partition(s) to delete:")
        for i, date in enumerate(old_partitions[:10], 1):
            count = spark.table(BRONZE_WORK_UNITS).filter(F.col("ingestion_date") == date).count()
            print(f"  {i}. {date} → {count:,} rows")
        if len(old_partitions) > 10:
            print(f"  ... and {len(old_partitions) - 10} more")
        
        # Calculate total rows to delete
        total_to_delete = (
            spark.table(BRONZE_WORK_UNITS)
            .filter(F.col("ingestion_date") < cutoff_date)
            .count()
        )
        print(f"\nTotal rows to delete: {total_to_delete:,}")
        
        # SAFETY CHECK: Confirm Silver is up-to-date
        print("\n⚠️  SAFETY CHECK: Ensure Silver has processed these dates")
        print("    If unsure, set RUN_RETENTION_CLEANUP = False and verify first.")
        
        # Delete old partitions
        bronze_delta = DeltaTable.forName(spark, BRONZE_WORK_UNITS)
        
        print(f"\nDeleting partitions older than {cutoff_date}...")
        bronze_delta.delete(F.col("ingestion_date") < cutoff_date)
        
        print(f"✓ Deleted {total_to_delete:,} rows from {len(old_partitions)} partition(s)")
        
        # VACUUM to reclaim storage (removes old files)
        print("\nRunning VACUUM to reclaim storage...")
        spark.sql(f"VACUUM {BRONZE_WORK_UNITS} RETAIN 168 HOURS")  # 7 days
        print("✓ VACUUM complete - storage reclaimed")
        
        # Repeat for integration tables
        print("\n" + "="*70)
        print("Cleaning integration tables...")
        
        for table in [BRONZE_ENG_DELETES, BRONZE_MONTHLY]:
            print(f"\nProcessing {table}...")
            old_count = (
                spark.table(table)
                .filter(F.col("ingestion_date") < cutoff_date)
                .count()
            )
            
            if old_count > 0:
                DeltaTable.forName(spark, table).delete(F.col("ingestion_date") < cutoff_date)
                print(f"  ✓ Deleted {old_count:,} rows")
                spark.sql(f"VACUUM {table} RETAIN 168 HOURS")
            else:
                print(f"  ✓ No old data to delete")
        
        print(f"\n{'='*70}")
        print("✓ Bronze retention cleanup complete")
        
        # Show final stats
        bronze_remaining = spark.table(BRONZE_WORK_UNITS).count()
        print(f"Bronze rows remaining: {bronze_remaining:,}")
        
else:
    print("Retention cleanup disabled (set RUN_RETENTION_CLEANUP = True to enable)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
