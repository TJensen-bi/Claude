# Databricks notebook source
# MAGIC %md
# MAGIC # Log Copy Metrics
# MAGIC
# MAGIC Logs copy activity metrics to ControlLakehouse.
# MAGIC
# MAGIC **Called by pipeline after each CopyTable activity (both success and failure).**
# MAGIC
# MAGIC ## Parameters:
# MAGIC - pipeline_run_id: Pipeline execution ID
# MAGIC - pipeline_name: Name of the pipeline
# MAGIC - table_schema: Source table schema
# MAGIC - table_name: Table name
# MAGIC - rows_read: Number of rows read
# MAGIC - rows_copied: Number of rows copied
# MAGIC - data_read: Bytes read from source
# MAGIC - data_written: Bytes written to sink
# MAGIC - copy_duration: Copy duration in seconds
# MAGIC - execution_status: Success or Failed
# MAGIC - error_message: Error message (if failed)

# COMMAND ----------

# PARAMETERS CELL - Toggle this as parameter cell
# Parameters (these will be passed from the pipeline)
pipeline_run_id = ""
pipeline_name = ""
table_schema = ""
table_name = ""
rows_read = "0"
rows_copied = "0"
data_read = "0"
data_written = "0"
copy_duration = "0"
execution_status = "Success"
error_message = ""

# COMMAND ----------

# Log_Copy_Metrics Notebook
# Logs copy activity metrics to ControlLakehouse Delta table
# Handles both SUCCESS and FAILURE cases

from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import datetime
import uuid
import time

print("="*60)
print("Logging Copy Activity Metrics")
print("="*60)

# Convert string parameters to appropriate types
try:
    rows_read_int = int(float(rows_read)) if rows_read and rows_read != "" else 0
except:
    rows_read_int = 0

try:
    rows_copied_int = int(float(rows_copied)) if rows_copied and rows_copied != "" else 0
except:
    rows_copied_int = 0

try:
    data_read_int = int(float(data_read)) if data_read and data_read != "" else 0
except:
    data_read_int = 0

try:
    data_written_int = int(float(data_written)) if data_written and data_written != "" else 0
except:
    data_written_int = 0

try:
    copy_duration_int = int(float(copy_duration)) if copy_duration and copy_duration != "" else 0
except:
    copy_duration_int = 0

# Calculate metrics
data_read_mb = float(data_read_int) / (1024.0 * 1024.0) if data_read_int > 0 else 0.0
data_written_mb = float(data_written_int) / (1024.0 * 1024.0) if data_written_int > 0 else 0.0
throughput_mbps = float(data_written_mb) / float(copy_duration_int) if copy_duration_int > 0 else 0.0

# Display what we're logging
print(f"\nPipeline: {pipeline_name}")
print(f"Run ID: {pipeline_run_id}")
print(f"Table: {table_schema}.{table_name}" if table_schema else f"Table: {table_name}")
print(f"Status: {execution_status}")

if execution_status == "Success":
    print(f"Rows Copied: {rows_copied_int:,}")
    print(f"Data Written: {data_written_mb:.2f} MB")
    print(f"Duration: {copy_duration_int} seconds")
    print(f"Throughput: {throughput_mbps:.2f} MB/s")
else:
    print(f"Error: {error_message}")

# IMPORTANT: UPDATE THESE WITH YOUR ACTUAL IDs
# Get these from your ControlLakehouse URL
WORKSPACE_ID = "YOUR-WORKSPACE-ID"  # Replace with your workspace ID
CONTROL_LAKEHOUSE_ID = "YOUR-CONTROL-LAKEHOUSE-ID"  # Replace with your ControlLakehouse ID

control_path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{CONTROL_LAKEHOUSE_ID}/Tables/CopyActivityLog"

# Define the schema explicitly
schema = StructType([
    StructField("LogID", StringType(), False),
    StructField("PipelineRunID", StringType(), False),
    StructField("PipelineName", StringType(), True),
    StructField("TableSchema", StringType(), True),
    StructField("TableName", StringType(), False),
    StructField("RowsRead", LongType(), True),
    StructField("RowsCopied", LongType(), True),
    StructField("DataReadMB", DoubleType(), True),
    StructField("DataWrittenMB", DoubleType(), True),
    StructField("CopyDurationSeconds", IntegerType(), True),
    StructField("ThroughputMBps", DoubleType(), True),
    StructField("ExecutionStatus", StringType(), True),
    StructField("ErrorMessage", StringType(), True),
    StructField("SourceDatabase", StringType(), True),
    StructField("TargetPath", StringType(), True),
    StructField("StartDateTime", TimestampType(), True),
    StructField("EndDateTime", TimestampType(), True),
    StructField("CreatedDate", TimestampType(), False)
])

# Create log entry with explicit types
log_data = [(
    str(uuid.uuid4()),
    str(pipeline_run_id),
    str(pipeline_name) if pipeline_name else None,
    str(table_schema) if table_schema else None,
    str(table_name),
    int(rows_read_int),
    int(rows_copied_int),
    float(round(data_read_mb, 2)),
    float(round(data_written_mb, 2)),
    int(copy_duration_int),
    float(round(throughput_mbps, 2)),
    str(execution_status),
    str(error_message) if error_message else None,
    "AdventureWorks",
    f"adventureworks/{datetime.now().strftime('%Y%m%d')}/{table_name}.parquet",
    datetime.now(),
    datetime.now(),
    datetime.now()
)]

# Create DataFrame with explicit schema
log_df = spark.createDataFrame(log_data, schema)

# Retry logic for concurrent writes
max_retries = 5
retry_count = 0
success = False

while retry_count < max_retries and not success:
    try:
        # Append to Delta table with schema merging enabled
        log_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(control_path)

        success = True

        if execution_status == "Success":
            print(f"\n✓ Success metrics logged to CopyActivityLog")
        else:
            print(f"\n✓ Failure logged to CopyActivityLog")

    except Exception as e:
        error_str = str(e)

        # Check if it's a concurrency error
        if "ProtocolChangedException" in error_str or "ConcurrentAppendException" in error_str:
            retry_count += 1
            if retry_count < max_retries:
                # Exponential backoff with jitter
                wait_time = (2 ** retry_count) + (retry_count * 0.1)
                print(f"\n⚠ Concurrent write detected. Retry {retry_count}/{max_retries} in {wait_time:.1f}s...")
                time.sleep(wait_time)
            else:
                print(f"\n✗ Failed after {max_retries} retries due to concurrency conflict")
                raise
        else:
            # Different error, don't retry
            print(f"\n✗ Error logging metrics: {error_str}")
            raise

print("="*60)
