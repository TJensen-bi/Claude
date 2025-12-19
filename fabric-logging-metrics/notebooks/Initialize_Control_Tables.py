# Databricks notebook source
# MAGIC %md
# MAGIC # Initialize Control Tables
# MAGIC
# MAGIC Creates Delta Lake tables in ControlLakehouse for storing pipeline metrics and validation results.
# MAGIC
# MAGIC **Run this notebook once during initial setup.**
# MAGIC
# MAGIC ## Tables Created:
# MAGIC - CopyActivityLog: Copy activity metrics
# MAGIC - BronzeValidationLog: Row count validation results
# MAGIC - MedallionLayerValidation: Cross-layer validation
# MAGIC - ChecksumValidation: Financial data checksums

# COMMAND ----------

# Initialize_Control_Tables Notebook
# Run this once to create your control/logging tables in Delta format

from pyspark.sql.types import *
from datetime import datetime

print("="*60)
print("Initializing Control Tables for Fabric Logging Solution")
print("="*60)

# Define schema for Copy Activity Log
copy_activity_schema = StructType([
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

# Define schema for Bronze Validation Log
bronze_validation_schema = StructType([
    StructField("ValidationID", StringType(), False),
    StructField("PipelineRunID", StringType(), False),
    StructField("TableName", StringType(), False),
    StructField("SourceRowCount", LongType(), True),
    StructField("BronzeRowCount", LongType(), True),
    StructField("RowCountMatch", BooleanType(), True),
    StructField("MismatchCount", LongType(), True),
    StructField("ValidationDateTime", TimestampType(), False),
    StructField("ValidationStatus", StringType(), True)
])

# Define schema for Medallion Layer Validation
medallion_validation_schema = StructType([
    StructField("ValidationID", StringType(), False),
    StructField("PipelineRunID", StringType(), False),
    StructField("TableName", StringType(), False),
    StructField("BronzeRowCount", LongType(), True),
    StructField("SilverRowCount", LongType(), True),
    StructField("GoldRowCount", LongType(), True),
    StructField("BronzeToSilverMatch", BooleanType(), True),
    StructField("SilverToGoldMatch", BooleanType(), True),
    StructField("ValidationDateTime", TimestampType(), False)
])

# Define schema for Data Quality Checksums
checksum_validation_schema = StructType([
    StructField("ChecksumID", StringType(), False),
    StructField("PipelineRunID", StringType(), False),
    StructField("TableName", StringType(), False),
    StructField("ColumnName", StringType(), False),
    StructField("SourceSum", DoubleType(), True),
    StructField("BronzeSum", DoubleType(), True),
    StructField("Difference", DoubleType(), True),
    StructField("ChecksumMatch", BooleanType(), True),
    StructField("ValidationDateTime", TimestampType(), False)
])

print("\n[1/4] Creating CopyActivityLog table...")
copy_activity_df = spark.createDataFrame([], copy_activity_schema)
copy_activity_df.write.format("delta").mode("overwrite").save("Tables/CopyActivityLog")
print("✓ CopyActivityLog created")

print("\n[2/4] Creating BronzeValidationLog table...")
bronze_validation_df = spark.createDataFrame([], bronze_validation_schema)
bronze_validation_df.write.format("delta").mode("overwrite").save("Tables/BronzeValidationLog")
print("✓ BronzeValidationLog created")

print("\n[3/4] Creating MedallionLayerValidation table...")
medallion_validation_df = spark.createDataFrame([], medallion_validation_schema)
medallion_validation_df.write.format("delta").mode("overwrite").save("Tables/MedallionLayerValidation")
print("✓ MedallionLayerValidation created")

print("\n[4/4] Creating ChecksumValidation table...")
checksum_validation_df = spark.createDataFrame([], checksum_validation_schema)
checksum_validation_df.write.format("delta").mode("overwrite").save("Tables/ChecksumValidation")
print("✓ ChecksumValidation created")

print("\n" + "="*60)
print("✓ ALL CONTROL TABLES CREATED SUCCESSFULLY!")
print("="*60)
print("\nTables created in ControlLakehouse:")
print("  1. CopyActivityLog")
print("  2. BronzeValidationLog")
print("  3. MedallionLayerValidation")
print("  4. ChecksumValidation")
print("\nYou can now proceed to create the logging notebooks.")
print("="*60)
