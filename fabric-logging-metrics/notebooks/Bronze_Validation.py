# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Validation
# MAGIC
# MAGIC Validates data loaded into Bronze layer by comparing row counts with source database.
# MAGIC
# MAGIC **Called by pipeline after ForEachTable completes.**
# MAGIC
# MAGIC ## Features:
# MAGIC - Row count validation (source vs. Bronze)
# MAGIC - Logs results to BronzeValidationLog
# MAGIC - Raises exception if validation fails (to fail pipeline)
# MAGIC
# MAGIC ## Lakehouses Required:
# MAGIC - Bronze (default) - to read parquet files
# MAGIC - ControlLakehouse (attached) - to write validation logs

# COMMAND ----------

# PARAMETERS CELL - Toggle this as parameter cell
# Parameters
pipeline_run_id = "manual_run"
execution_date = "20251218"
control_lakehouse_workspace_id = "YOUR-WORKSPACE-ID"
control_lakehouse_id = "YOUR-CONTROL-LAKEHOUSE-ID"

# COMMAND ----------

# Configuration - UPDATE THESE VALUES FOR YOUR ENVIRONMENT

# JDBC connection to AdventureWorks source database
jdbc_url = "jdbc:sqlserver://YOUR-SERVER.database.windows.net:1433;database=AdventureWorks"
jdbc_properties = {
    "user": "YOUR-USERNAME",
    "password": "YOUR-PASSWORD",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Tables to validate
tables_to_validate = [
    "Address",
    "Customer",
    "Product",
    "ProductCategory",
    "ProductDescription",
    "ProductModel",
    "SalesOrderDetail",
    "SalesOrderHeader"
]

print("✓ Configuration loaded")
print(f"  Tables to validate: {len(tables_to_validate)}")
print(f"  Execution date: {execution_date}")
print(f"  Bronze path: Files/adventureworks/{execution_date}/")

# Quick check: Try to read one file to verify Bronze lakehouse access
try:
    test_file = f"Files/adventureworks/{execution_date}/{tables_to_validate[0]}.parquet"
    print(f"\n  Testing Bronze access with: {test_file}")
    test_df = spark.read.parquet(test_file)
    print(f"  ✓ Bronze lakehouse is accessible")
    print(f"  ✓ Found {test_df.count():,} rows in {tables_to_validate[0]}")
except Exception as e:
    print(f"  ⚠ Cannot read test file: {str(e)}")
    print(f"  Make sure Bronze lakehouse is attached and files exist")

# COMMAND ----------

# Bronze_Validation Notebook
# Validates data loaded into Bronze layer and logs to ControlLakehouse

from pyspark.sql import Row
from datetime import datetime
import uuid

print("="*60)
print("Bronze Layer Validation")
print("="*60)
print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Execution Date: {execution_date}")

# Verify execution_date is set
if not execution_date or execution_date == "":
    execution_date = datetime.now().strftime('%Y%m%d')
    print(f"⚠ execution_date was empty, using today: {execution_date}")

print(f"Bronze path: Files/adventureworks/{execution_date}/")
print("="*60)

# Check what files actually exist using PySpark
print("\nChecking Bronze layer files...")
try:
    # Try to read any parquet files in the date folder
    test_path = f"Files/adventureworks/{execution_date}/*.parquet"
    files_df = spark.read.format("binaryFile").load(test_path)
    file_count = files_df.count()
    print(f"✓ Found {file_count} parquet files in adventureworks/{execution_date}/")

    # Show the file names
    file_paths = files_df.select("path").collect()
    for row in file_paths:
        file_name = row.path.split("/")[-1]
        print(f"  - {file_name}")

except Exception as e:
    print(f"✗ Cannot read files from Bronze layer: {str(e)}")
    print(f"\n⚠ Make sure:")
    print(f"  1. Bronze lakehouse is attached to this notebook")
    print(f"  2. Files exist in: Files/adventureworks/{execution_date}/")
    print(f"  3. The pipeline copied files to this location")

print("\n" + "="*60)

validation_results = []

for table_name in tables_to_validate:
    print(f"\n[{tables_to_validate.index(table_name) + 1}/{len(tables_to_validate)}] Validating {table_name}...")

    try:
        # Include the date folder in path
        bronze_path = f"Files/adventureworks/{execution_date}/{table_name}.parquet"
        print(f"  Reading: {bronze_path}")

        bronze_df = spark.read.parquet(bronze_path)
        bronze_count = bronze_df.count()

        # Get source row count
        source_query = f"(SELECT COUNT(*) as row_count FROM SalesLT.{table_name}) AS src"
        source_df = spark.read.jdbc(
            url=jdbc_url,
            table=source_query,
            properties=jdbc_properties
        )
        source_count = source_df.collect()[0]['row_count']

        # Compare counts
        match = (source_count == bronze_count)
        mismatch = abs(source_count - bronze_count)

        # Create validation entry
        validation_entry = Row(
            ValidationID=str(uuid.uuid4()),
            PipelineRunID=pipeline_run_id,
            TableName=table_name,
            SourceRowCount=source_count,
            BronzeRowCount=bronze_count,
            RowCountMatch=match,
            MismatchCount=mismatch,
            ValidationDateTime=datetime.now(),
            ValidationStatus="Pass" if match else "Fail"
        )

        validation_results.append(validation_entry)

        status_symbol = "✓" if match else "✗"
        print(f"  {status_symbol} Source: {source_count:,} | Bronze: {bronze_count:,} | Match: {match}")

    except Exception as e:
        print(f"  ✗ Error: {str(e)}")

        error_entry = Row(
            ValidationID=str(uuid.uuid4()),
            PipelineRunID=pipeline_run_id,
            TableName=table_name,
            SourceRowCount=0,
            BronzeRowCount=0,
            RowCountMatch=False,
            MismatchCount=0,
            ValidationDateTime=datetime.now(),
            ValidationStatus="Error"
        )
        validation_results.append(error_entry)

# Write validation results to Delta table (with retry logic for concurrent writes)
if validation_results:
    import time
    validation_df = spark.createDataFrame(validation_results)

    # Build path to ControlLakehouse
    control_path = f"abfss://{control_lakehouse_workspace_id}@onelake.dfs.fabric.microsoft.com/{control_lakehouse_id}/Tables/BronzeValidationLog"

    max_retries = 3
    retry_count = 0
    success = False

    while retry_count < max_retries and not success:
        try:
            validation_df.write.format("delta").mode("append").save(control_path)
            success = True
            print("\n✓ Results saved to BronzeValidationLog in ControlLakehouse")
        except Exception as e:
            if "ProtocolChangedException" in str(e) or "ConcurrentAppendException" in str(e):
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 2 ** retry_count
                    print(f"\n⚠ Concurrent write detected. Retry {retry_count}/{max_retries} in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"\n✗ Failed to save results after {max_retries} retries")
                    raise
            else:
                raise

# Print summary
total_tables = len(validation_results)
passed = sum(1 for v in validation_results if v.ValidationStatus == "Pass")
failed = sum(1 for v in validation_results if v.ValidationStatus in ["Fail", "Error"])

print("\n" + "="*60)
print("VALIDATION SUMMARY")
print("="*60)
print(f"Total Tables: {total_tables}")
print(f"Passed: {passed}")
print(f"Failed: {failed}")
print(f"Success Rate: {(passed/total_tables)*100:.1f}%")
print("="*60)

# Raise error if any validations failed (this will make the pipeline fail)
if failed > 0:
    raise Exception(f"Bronze validation failed for {failed} table(s)")
else:
    print("\n✓ All validations passed successfully!")
