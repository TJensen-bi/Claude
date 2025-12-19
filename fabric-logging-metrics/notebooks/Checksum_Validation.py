# Databricks notebook source
# MAGIC %md
# MAGIC # Checksum Validation
# MAGIC
# MAGIC Validates numeric column totals for financial data accuracy.
# MAGIC
# MAGIC **Optional - Run manually or schedule separately.**
# MAGIC
# MAGIC ## Features:
# MAGIC - Validates sum of numeric columns (e.g., LineTotal, TotalDue)
# MAGIC - Compares source vs. Bronze sums with tolerance
# MAGIC - Logs results to ChecksumValidation table
# MAGIC
# MAGIC ## Use Cases:
# MAGIC - Financial data accuracy verification
# MAGIC - Detecting data corruption during transfer
# MAGIC - Compliance and audit requirements

# COMMAND ----------

# PARAMETERS CELL
pipeline_run_id = "manual_run"
execution_date = "20251218"
control_lakehouse_workspace_id = "YOUR-WORKSPACE-ID"
control_lakehouse_id = "YOUR-CONTROL-LAKEHOUSE-ID"

# COMMAND ----------

# Configuration - UPDATE THESE VALUES

jdbc_url = "jdbc:sqlserver://YOUR-SERVER.database.windows.net:1433;database=AdventureWorks"
jdbc_properties = {
    "user": "YOUR-USERNAME",
    "password": "YOUR-PASSWORD",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Tables and columns to validate with checksums
checksum_config = {
    "SalesOrderDetail": ["OrderQty", "UnitPrice", "UnitPriceDiscount", "LineTotal"],
    "SalesOrderHeader": ["SubTotal", "TaxAmt", "Freight", "TotalDue"]
}

print("✓ Checksum configuration loaded")

# COMMAND ----------

# Checksum_Validation Notebook
# Validates numeric totals for financial data accuracy

from pyspark.sql.functions import sum, col
from pyspark.sql import Row
from datetime import datetime
import uuid
import time

print("="*60)
print("Financial Data Checksum Validation")
print("="*60)
print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Execution Date: {execution_date}")
print("="*60)

checksum_results = []

for table_name, columns in checksum_config.items():
    print(f"\n--- Validating {table_name} ---")

    try:
        # Read source and bronze
        source_query = f"(SELECT * FROM SalesLT.{table_name}) AS src"
        source_df = spark.read.jdbc(url=jdbc_url, table=source_query, properties=jdbc_properties)

        bronze_path = f"Files/adventureworks/{execution_date}/{table_name}.parquet"
        bronze_df = spark.read.parquet(bronze_path)

        # Validate each numeric column
        for column_name in columns:
            try:
                source_sum = source_df.select(sum(col(column_name))).collect()[0][0]
                bronze_sum = bronze_df.select(sum(col(column_name))).collect()[0][0]

                source_sum_float = float(source_sum or 0)
                bronze_sum_float = float(bronze_sum or 0)
                difference = abs(source_sum_float - bronze_sum_float)

                # Tolerance for floating point precision
                tolerance = 0.01
                match = difference < tolerance

                # Create checksum entry
                checksum_entry = Row(
                    ChecksumID=str(uuid.uuid4()),
                    PipelineRunID=pipeline_run_id,
                    TableName=table_name,
                    ColumnName=column_name,
                    SourceSum=source_sum_float,
                    BronzeSum=bronze_sum_float,
                    Difference=difference,
                    ChecksumMatch=match,
                    ValidationDateTime=datetime.now()
                )

                checksum_results.append(checksum_entry)

                symbol = "✓" if match else "✗"
                print(f"  {symbol} {column_name}: ${source_sum_float:,.2f} | ${bronze_sum_float:,.2f} | Diff: ${difference:.2f}")

            except Exception as e:
                print(f"  ✗ Error validating {column_name}: {str(e)}")

    except Exception as e:
        print(f"✗ Error loading {table_name}: {str(e)}")

# Write checksum results to Delta table
if checksum_results:
    checksum_df = spark.createDataFrame(checksum_results)

    # Build path to ControlLakehouse
    control_path = f"abfss://{control_lakehouse_workspace_id}@onelake.dfs.fabric.microsoft.com/{control_lakehouse_id}/Tables/ChecksumValidation"

    max_retries = 3
    retry_count = 0
    success = False

    while retry_count < max_retries and not success:
        try:
            checksum_df.write.format("delta").mode("append").save(control_path)
            success = True
            print("\n✓ Results saved to ChecksumValidation")
        except Exception as e:
            if "ProtocolChangedException" in str(e) or "ConcurrentAppendException" in str(e):
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 2 ** retry_count
                    print(f"\n⚠ Concurrent write detected. Retry {retry_count}/{max_retries} in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise
            else:
                raise

# Print summary
total_checks = len(checksum_results)
passed = sum(1 for c in checksum_results if c.ChecksumMatch)

print(f"\n{'='*60}")
print("CHECKSUM VALIDATION SUMMARY")
print(f"{'='*60}")
print(f"Total Checks: {total_checks}")
print(f"Passed: {passed}")
print(f"Failed: {total_checks - passed}")
print("="*60)

if passed < total_checks:
    raise Exception(f"Checksum validation failed for {total_checks - passed} column(s)")
else:
    print("\n✓ All checksum validations passed!")
