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

# # Copper Transform — Decompress JSON from Bi_WorkUnits
# 
# **Purpose**: Decompress and transform the `serializedRowsandValuesZip` column from `copper.Bi_WorkUnits`.
# 
# **Compression Details**:
# - Algorithm: GZIP (DEFLATE) - SQL Server standard
# - Encoding: UTF-8
# - Source: SQL Server `SerializeAndCompressObject` method
# - Format: Newtonsoft.Json serialization + GZipStream compression
# 
# **Transformation Steps**:
# 1. Read compressed data from Copper
# 2. Decompress GZIP binary data
# 3. Decode UTF-8 bytes to JSON string
# 4. Parse JSON and extract fields
# 5. Write transformed data to Silver layer
# 
# **Tables**:
# | Table | Role |
# |---|---|
# | `copper.Bi_WorkUnits` | Source table with compressed JSON |
# | `silver.Bi_WorkUnits_Expanded` | Target table with decompressed/parsed data |

# CELL ********************

# Cell 1 — Configuration & Imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from delta.tables import DeltaTable
import gzip
import base64
import json

spark = SparkSession.builder.getOrCreate()

# Performance settings
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

# ---------------------------------------------------------------------------
# TABLE NAMES
# ---------------------------------------------------------------------------
COPPER_WORK_UNITS = "copper.Bi_WorkUnits"  # Source
SILVER_WORK_UNITS = "silver.Bi_WorkUnits_Expanded"  # Target

# Column containing compressed JSON
COMPRESSED_COLUMN = "serializedRowsandValuesZip"

print("✓ Configuration loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Create Decompression UDF
# 
# Create a PySpark UDF to decompress GZIP data and decode UTF-8 JSON strings.
# 
# **Process**:
# 1. Receive binary data (from SQL Server VARBINARY column)
# 2. Decompress using GZIP
# 3. Decode UTF-8 bytes to string
# 4. Return JSON string for further parsing

# CELL ********************

# Define UDF to decompress GZIP and decode UTF-8
def decompress_gzip_utf8(compressed_bytes):
    """
    Decompress GZIP binary data and decode to UTF-8 string.
    
    Args:
        compressed_bytes: Binary data (bytes) compressed with GZIP
    
    Returns:
        str: Decompressed JSON string, or None if decompression fails
    """
    if compressed_bytes is None:
        return None
    
    try:
        # Decompress GZIP data
        decompressed_bytes = gzip.decompress(compressed_bytes)
        
        # Decode UTF-8 to string
        json_string = decompressed_bytes.decode('utf-8')
        
        return json_string
    
    except Exception as e:
        # Return None for corrupted/invalid data
        # In production, you might want to log this
        return None

# Register UDF with Spark
decompress_udf = F.udf(decompress_gzip_utf8, StringType())

print("✓ Decompression UDF registered")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Read Sample Data from Copper
# 
# Read a small sample to test decompression and understand the JSON structure.

# CELL ********************

# Read sample from Copper (limit for testing)
SAMPLE_SIZE = 10  # Adjust for testing

df_copper_sample = (
    spark.table(COPPER_WORK_UNITS)
    .select(
        "ID",
        "MonthlyDataID",
        COMPRESSED_COLUMN
    )
    .filter(F.col(COMPRESSED_COLUMN).isNotNull())  # Only rows with data
    .limit(SAMPLE_SIZE)
)

sample_count = df_copper_sample.count()
print(f"Sample records with compressed data: {sample_count:,}")

# Show schema
df_copper_sample.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Test Decompression
# 
# Apply the UDF to decompress the column and inspect the JSON structure.

# CELL ********************

# Apply decompression UDF
df_decompressed = df_copper_sample.withColumn(
    "decompressed_json",
    decompress_udf(F.col(COMPRESSED_COLUMN))
)

# Check results
print("\nDecompression Results:")
df_decompressed.select(
    "ID",
    "MonthlyDataID",
    F.col("decompressed_json").substr(1, 100).alias("json_preview")  # First 100 chars
).show(5, truncate=False)

# Check for decompression failures
failed_count = df_decompressed.filter(F.col("decompressed_json").isNull()).count()
print(f"\nDecompression failures: {failed_count} / {sample_count}")

if failed_count == 0:
    print("✓ All samples decompressed successfully")
else:
    print(f"⚠ {failed_count} rows failed to decompress")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3b — Inspect Full JSON Structure
# 
# Display the complete decompressed JSON to understand its structure.

# CELL ********************

# Display full JSON from first record to understand structure
first_json = df_decompressed.select("decompressed_json").first()[0]

if first_json:
    print("=" * 80)
    print("FULL JSON CONTENT (First Record):")
    print("=" * 80)
    print(first_json)
    print("=" * 80)
    
    # Try to parse and pretty-print
    try:
        import json
        parsed = json.loads(first_json)
        print("\nPRETTY-PRINTED JSON:")
        print("=" * 80)
        print(json.dumps(parsed, indent=2))
        print("=" * 80)
        
        # Show structure info
        print("\nJSON STRUCTURE INFO:")
        print(f"Type: {type(parsed).__name__}")
        if isinstance(parsed, dict):
            print(f"Keys: {list(parsed.keys())}")
            for key, value in parsed.items():
                print(f"  - {key}: {type(value).__name__}", end="")
                if isinstance(value, list) and len(value) > 0:
                    print(f" (length={len(value)}, first item type={type(value[0]).__name__})")
                elif isinstance(value, dict):
                    print(f" (keys={list(value.keys())})")
                else:
                    print()
        elif isinstance(parsed, list):
            print(f"Array length: {len(parsed)}")
            if len(parsed) > 0:
                print(f"First item type: {type(parsed[0]).__name__}")
                if isinstance(parsed[0], dict):
                    print(f"First item keys: {list(parsed[0].keys())}")
    except Exception as e:
        print(f"\n⚠ JSON parsing failed: {e}")
else:
    print("⚠ No JSON data found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4 — Parse JSON Structure
# 
# **Action Required**: Examine the decompressed JSON above to understand its structure.
# 
# Once you know the JSON schema, update the code below to parse it correctly.
# 
# Common patterns:
# - **Simple JSON object**: `{"field1": "value1", "field2": "value2"}`
# - **Array of objects**: `[{"row": 1, "data": {...}}, {...}]`
# - **Nested structure**: `{"rows": [{...}], "values": [{...}]}`

# CELL ********************

# Parse JSON using from_json()
# Based on actual JSON structure: Array of metric types with monthly values

from pyspark.sql.types import IntegerType, DoubleType

# JSON Structure:
# [
#   {
#     "Type": 0,
#     "TypeName": "Activity/Volume",
#     "MonthlyValues": [
#       {"Month": 2, "Year": 2019, "Value": 3.6667},
#       ...
#     ]
#   },
#   ...
# ]

# Define schema for MonthlyValues nested array
monthly_value_schema = StructType([
    StructField("Month", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Value", DoubleType(), True)
])

# Define schema for each metric type object
metric_type_schema = StructType([
    StructField("Type", IntegerType(), True),
    StructField("TypeName", StringType(), True),
    StructField("MonthlyValues", ArrayType(monthly_value_schema), True)
])

# Top-level is an array of metric types
json_schema = ArrayType(metric_type_schema)

# Parse JSON - top level is an array
df_parsed = df_decompressed.withColumn(
    "parsed_json",
    F.from_json(F.col("decompressed_json"), json_schema)
)

# Display parsed structure
print("Parsed JSON structure:")
df_parsed.select("ID", "parsed_json").show(2, truncate=False)

# Check schema
print("\nSchema:")
df_parsed.select("parsed_json").printSchema()

# Count metric types per record
df_parsed_with_count = df_parsed.withColumn(
    "metric_count",
    F.size(F.col("parsed_json"))
)
print("\nMetric types per record:")
df_parsed_with_count.select("ID", "metric_count").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5 — Explode and Flatten Data
# 
# Transform the nested JSON into a flat table structure.
# 
# **Transformation steps:**
# 1. Explode outer array → one row per metric type (Type, TypeName)
# 2. Explode MonthlyValues array → one row per month/year/value
# 3. Result: Each row represents one metric value for a specific month/year

# CELL ********************

# Step 1: Explode outer array (metric types)
df_exploded_types = df_parsed.select(
    "ID",
    "MonthlyDataID",
    F.explode(F.col("parsed_json")).alias("metric_type")
)

# Step 2: Extract Type and TypeName, then explode MonthlyValues
df_exploded_months = df_exploded_types.select(
    "ID",
    "MonthlyDataID",
    F.col("metric_type.Type").alias("Type"),
    F.col("metric_type.TypeName").alias("TypeName"),
    F.explode(F.col("metric_type.MonthlyValues")).alias("monthly_value")
)

# Step 3: Flatten monthly values
df_flattened = df_exploded_months.select(
    "ID",
    "MonthlyDataID",
    "Type",
    "TypeName",
    F.col("monthly_value.Month").alias("Month"),
    F.col("monthly_value.Year").alias("Year"),
    F.col("monthly_value.Value").alias("Value")
)

print("Flattened data (sample):")
df_flattened.show(10)

# Show statistics
total_rows = df_flattened.count()
unique_types = df_flattened.select("Type", "TypeName").distinct().count()

print(f"\nTransformation Results:")
print(f"  Total rows: {total_rows:,}")
print(f"  Unique metric types: {unique_types}")
print(f"  Expansion ratio: {total_rows / sample_count:.1f}x")

# Show metric type distribution
print("\nMetric types found:")
df_flattened.groupBy("Type", "TypeName").count().orderBy("Type").show(20, truncate=False)

# Show sample data by metric type
print("\nSample values for first few metric types:")
df_flattened.filter(F.col("Type").isin(0, 1, 2, 4, 5)).orderBy("Type", "Year", "Month").show(15)

# Verify data quality
null_check = df_flattened.select([
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_flattened.columns
]).collect()[0].asDict()

print("\nNull value check:")
for col, null_count in null_check.items():
    print(f"  {col}: {null_count} nulls")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6 — Process Full Dataset
# 
# Once the sample works correctly, process the full Copper table.
# 
# **Note**: For very large datasets, consider processing in batches or using incremental processing.

# CELL ********************

# Set to True when ready to process full dataset
PROCESS_FULL_DATASET = True

if PROCESS_FULL_DATASET:
    print("Processing full Copper dataset...")
    
    # Read all data from Copper
    df_copper_full = (
        spark.table(COPPER_WORK_UNITS)
        .select(
            "ID",
            "MonthlyDataID",
            COMPRESSED_COLUMN,
            "LastUpdateDate",  # Keep for tracking
            "ingestion_date"   # Keep if needed
        )
        .filter(F.col(COMPRESSED_COLUMN).isNotNull())
    )
    
    total_count = df_copper_full.count()
    print(f"Total rows with compressed data: {total_count:,}")
    
    # Apply full transformation pipeline
    df_transformed = (
        df_copper_full
        # Step 1: Decompress
        .withColumn("decompressed_json", decompress_udf(F.col(COMPRESSED_COLUMN)))
        # Step 2: Parse JSON
        .withColumn("parsed_json", F.from_json(F.col("decompressed_json"), json_schema))
        # Step 3: Explode metric types
        .select(
            "ID",
            "MonthlyDataID",
            "LastUpdateDate",
            F.explode(F.col("parsed_json")).alias("metric_type")
        )
        # Step 4: Explode monthly values
        .select(
            "ID",
            "MonthlyDataID",
            "LastUpdateDate",
            F.col("metric_type.Type").alias("Type"),
            F.col("metric_type.TypeName").alias("TypeName"),
            F.explode(F.col("metric_type.MonthlyValues")).alias("monthly_value")
        )
        # Step 5: Flatten
        .select(
            "ID",
            "MonthlyDataID",
            "LastUpdateDate",
            "Type",
            "TypeName",
            F.col("monthly_value.Month").alias("Month"),
            F.col("monthly_value.Year").alias("Year"),
            F.col("monthly_value.Value").alias("Value")
        )
    )
    
    # Show results
    transformed_count = df_transformed.count()
    print(f"\nTransformed rows: {transformed_count:,}")
    print(f"Expansion ratio: {transformed_count / total_count:.2f}x")
    
else:
    print("⚠ Full dataset processing disabled")
    print("  Set PROCESS_FULL_DATASET = True when ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 7 — Write to Silver Layer
# 
# Write the transformed data to the Silver layer.

# CELL ********************

# Ensure silver schema exists
try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    print("✓ Schema 'silver' is ready")
except Exception as e:
    print(f"⚠ Schema check: {e}")

# Write to Silver table (only if full dataset processed)
if PROCESS_FULL_DATASET and 'df_transformed' in locals():
    print(f"\nWriting to Silver: {SILVER_WORK_UNITS}")
    
    # Write as Delta table
    (
        df_transformed
        .write
        .format("delta")
        .mode("overwrite")  # Change to "append" for incremental
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_WORK_UNITS)
    )
    
    # Verify
    silver_count = spark.table(SILVER_WORK_UNITS).count()
    print(f"✓ Silver table created with {silver_count:,} rows")
    
else:
    print("⚠ Skipping write - process full dataset first")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
