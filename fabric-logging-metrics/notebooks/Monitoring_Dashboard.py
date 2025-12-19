# Databricks notebook source
# MAGIC %md
# MAGIC # Monitoring Dashboard
# MAGIC
# MAGIC View pipeline performance and validation metrics.
# MAGIC
# MAGIC **Run this notebook on-demand to view current pipeline health.**
# MAGIC
# MAGIC ## Displays:
# MAGIC - Latest pipeline run summary
# MAGIC - Copy activity performance
# MAGIC - Validation status
# MAGIC - Overall statistics
# MAGIC - Checksum results (if available)

# COMMAND ----------

# Monitoring Dashboard
# View pipeline performance and validation metrics

from pyspark.sql.functions import *

print("="*80)
print("FABRIC PIPELINE MONITORING DASHBOARD")
print("="*80)

# 1. Latest Pipeline Run Summary
print("\n1. LATEST PIPELINE RUN")
print("-" * 80)
latest_run_df = spark.read.format("delta").load("Tables/CopyActivityLog") \
    .orderBy(col("CreatedDate").desc()) \
    .limit(1)

if latest_run_df.count() > 0:
    latest = latest_run_df.collect()[0]
    print(f"Pipeline: {latest['PipelineName']}")
    print(f"Run ID: {latest['PipelineRunID']}")
    print(f"Time: {latest['CreatedDate']}")
else:
    print("No pipeline runs found")

# 2. Copy Activity Metrics
print("\n2. COPY ACTIVITY PERFORMANCE (Last Run)")
print("-" * 80)
copy_metrics_df = spark.read.format("delta").load("Tables/CopyActivityLog") \
    .filter(col("PipelineRunID") == latest['PipelineRunID']) \
    .select("TableName", "RowsCopied", "DataWrittenMB", "CopyDurationSeconds", "ThroughputMBps", "ExecutionStatus")

copy_metrics_df.show(20, False)

# 3. Validation Summary
print("\n3. BRONZE VALIDATION STATUS (Last Run)")
print("-" * 80)
validation_df = spark.read.format("delta").load("Tables/BronzeValidationLog") \
    .filter(col("PipelineRunID") == latest['PipelineRunID']) \
    .select("TableName", "SourceRowCount", "BronzeRowCount", "RowCountMatch", "ValidationStatus")

validation_df.show(20, False)

# 4. Overall Statistics
print("\n4. OVERALL STATISTICS")
print("-" * 80)
stats_df = spark.read.format("delta").load("Tables/CopyActivityLog") \
    .filter(col("PipelineRunID") == latest['PipelineRunID']) \
    .agg(
        count("*").alias("TotalTables"),
        sum("RowsCopied").alias("TotalRows"),
        sum("DataWrittenMB").alias("TotalDataMB"),
        avg("ThroughputMBps").alias("AvgThroughputMBps"),
        sum(when(col("ExecutionStatus") == "Success", 1).otherwise(0)).alias("SuccessCount"),
        sum(when(col("ExecutionStatus") == "Failed", 1).otherwise(0)).alias("FailureCount")
    )

stats = stats_df.collect()[0]
print(f"Total Tables Processed: {stats['TotalTables']}")
print(f"Total Rows Copied: {stats['TotalRows']:,}")
print(f"Total Data Written: {stats['TotalDataMB']:.2f} MB")
print(f"Average Throughput: {stats['AvgThroughputMBps']:.2f} MB/s")
print(f"Successful Copies: {stats['SuccessCount']}")
print(f"Failed Copies: {stats['FailureCount']}")

# 5. Checksum Validation (if available)
checksum_count = spark.read.format("delta").load("Tables/ChecksumValidation").count()
if checksum_count > 0:
    print("\n5. CHECKSUM VALIDATION")
    print("-" * 80)
    checksum_df = spark.read.format("delta").load("Tables/ChecksumValidation") \
        .filter(col("PipelineRunID") == latest['PipelineRunID']) \
        .select("TableName", "ColumnName", "SourceSum", "BronzeSum", "ChecksumMatch")

    checksum_df.show(20, False)

print("\n" + "="*80)
print("✓ Dashboard refresh complete")
print("="*80)
