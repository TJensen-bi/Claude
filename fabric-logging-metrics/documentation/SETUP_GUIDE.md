# Complete Setup Guide: Microsoft Fabric Logging & Metrics Solution

This guide provides step-by-step instructions for implementing logging and metrics tracking in your Microsoft Fabric pipelines.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Step 1: Create Lakehouses](#step-1-create-lakehouses)
3. [Step 2: Create and Configure Notebooks](#step-2-create-and-configure-notebooks)
4. [Step 3: Initialize Control Tables](#step-3-initialize-control-tables)
5. [Step 4: Configure Pipeline](#step-4-configure-pipeline)
6. [Step 5: Test the Solution](#step-5-test-the-solution)
7. [Step 6: Set Up Monitoring](#step-6-set-up-monitoring)
8. [Verification Checklist](#verification-checklist)

---

## Prerequisites

Before starting, ensure you have:

- ✅ Microsoft Fabric workspace with appropriate permissions
- ✅ Fabric capacity or trial subscription
- ✅ Source database accessible from Fabric (e.g., Azure SQL Database)
- ✅ Basic knowledge of PySpark and Fabric pipelines
- ✅ Database credentials for source system

---

## Step 1: Create Lakehouses

### 1.1 Create ControlLakehouse

1. Navigate to your Fabric workspace
2. Click **+ New** → **Lakehouse**
3. Name: `ControlLakehouse`
4. Click **Create**
5. Wait for lakehouse to initialize

**Purpose**: Stores all logging and metrics Delta tables

### 1.2 Create Bronze Lakehouse (if not exists)

1. Click **+ New** → **Lakehouse**
2. Name: `Bronze`
3. Click **Create**

**Purpose**: Stores raw data from source systems

### 1.3 Note Lakehouse IDs

For both lakehouses:
1. Open the lakehouse
2. Look at the URL:
   ```
   https://app.fabric.microsoft.com/groups/{WORKSPACE_ID}/lakehouses/{LAKEHOUSE_ID}
   ```
3. Copy and save:
   - **Workspace ID**: The first GUID
   - **ControlLakehouse ID**: The second GUID from ControlLakehouse URL
   - **Bronze Lakehouse ID**: The second GUID from Bronze URL

---

## Step 2: Create and Configure Notebooks

### 2.1 Create Initialize_Control_Tables Notebook

1. In workspace, click **+ New** → **Notebook**
2. Name: `Initialize_Control_Tables`
3. Click **Add** → **Lakehouse** → Select `ControlLakehouse`
4. Copy code from `/notebooks/Initialize_Control_Tables.py`
5. Paste into notebook
6. Click **Save**

### 2.2 Create Log_Copy_Metrics Notebook

1. Click **+ New** → **Notebook**
2. Name: `Log_Copy_Metrics`
3. Click **Add** → **Lakehouse** → Select `ControlLakehouse`
4. Copy code from `/notebooks/Log_Copy_Metrics.py`

**Important Configuration:**

5. In the **first cell**, click **...** → **Toggle parameter cell**
6. Paste parameters code:
   ```python
   # Parameters
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
   ```

7. In **second cell**, paste the main logging code

8. **UPDATE** the write path with your IDs:
   ```python
   # Replace with YOUR actual IDs
   WORKSPACE_ID = "your-workspace-id"
   CONTROL_LAKEHOUSE_ID = "your-control-lakehouse-id"

   control_path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{CONTROL_LAKEHOUSE_ID}/Tables/CopyActivityLog"
   ```

9. Click **Save**

### 2.3 Create Bronze_Validation Notebook

1. Click **+ New** → **Notebook**
2. Name: `Bronze_Validation`
3. Click **Add** → **Lakehouse** → Select `ControlLakehouse`
4. Click **Add** → **Lakehouse** → Select `Bronze`
5. Right-click **Bronze** → **Set as default**
6. Copy code from `/notebooks/Bronze_Validation.py`

**Important Configuration:**

7. **First cell** - Toggle as parameter cell:
   ```python
   # Parameters
   pipeline_run_id = "manual_run"
   execution_date = "20251218"
   control_lakehouse_workspace_id = "your-workspace-id"
   control_lakehouse_id = "your-control-lakehouse-id"
   ```

8. **Second cell** - Configuration:
   ```python
   # UPDATE THESE VALUES
   jdbc_url = "jdbc:sqlserver://YOUR-SERVER.database.windows.net:1433;database=AdventureWorks"
   jdbc_properties = {
       "user": "YOUR-USERNAME",
       "password": "YOUR-PASSWORD",
       "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
   }

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
   ```

9. **Third cell** - Validation logic (paste from notebook file)

10. Click **Save**

### 2.4 Create Checksum_Validation Notebook (Optional)

1. Click **+ New** → **Notebook**
2. Name: `Checksum_Validation`
3. Attach both `ControlLakehouse` and `Bronze`
4. Set `Bronze` as default
5. Copy code from `/notebooks/Checksum_Validation.py`
6. Update JDBC configuration
7. Click **Save**

### 2.5 Create Monitoring_Dashboard Notebook (Optional)

1. Click **+ New** → **Notebook**
2. Name: `Monitoring_Dashboard`
3. Attach `ControlLakehouse`
4. Copy code from `/notebooks/Monitoring_Dashboard.py`
5. Click **Save**

---

## Step 3: Initialize Control Tables

### 3.1 Run Initialization

1. Open `Initialize_Control_Tables` notebook
2. Click **Run all**
3. Wait for execution to complete (30-60 seconds)
4. Verify success messages:
   ```
   ✓ CopyActivityLog created
   ✓ BronzeValidationLog created
   ✓ MedallionLayerValidation created
   ✓ ChecksumValidation created
   ```

### 3.2 Verify Tables Created

1. Go to `ControlLakehouse`
2. Expand **Tables** in left sidebar
3. Confirm you see:
   - CopyActivityLog
   - BronzeValidationLog
   - MedallionLayerValidation
   - ChecksumValidation

---

## Step 4: Configure Pipeline

### 4.1 Get Your Connection IDs

1. Open your existing source database connection in Fabric
2. Look at the URL or connection properties
3. Note the **connection ID**

### 4.2 Create Pipeline

1. In workspace, click **+ New** → **Data pipeline**
2. Name: `Adventureworks_Fabric_Logging`
3. Click **Create**

### 4.3 Import Pipeline JSON

1. Click **</>** (JSON view icon) in pipeline
2. Copy entire content from `/pipelines/Adventureworks_Fabric_Logging.json`
3. **Find and replace** the following placeholders:

   | Placeholder | Your Value | Where to Find |
   |-------------|------------|---------------|
   | `a11cefb9-35a1-4c1b-8384-cb931ebffb6c` | Your source DB connection ID | Connections in workspace |
   | `e26bbb09-4bf1-4e0a-9396-180457e77131` | Your workspace ID | URL when in workspace |
   | `60ea5356-3304-480f-a82d-08d0dddb799e` | Your Bronze lakehouse ID | Bronze lakehouse URL |
   | `305e8b78-51d7-4fbb-a9c9-3331e14b04ca` | Your Bronze connection ID | Bronze lakehouse properties |

4. Paste updated JSON
5. Click **OK**
6. Switch back to visual view

### 4.4 Verify Notebook References

In the pipeline, verify these activities exist:
- ✅ SetPipelineStartTime
- ✅ LookupTables
- ✅ ForEachTable
  - ✅ CopyTable
  - ✅ LogCopyMetrics (notebook)
  - ✅ LogCopyFailure (notebook)
- ✅ ValidateBronzeLayer (notebook)

### 4.5 Save and Publish

1. Click **Save** (disk icon)
2. Click **Publish** (if available)

---

## Step 5: Test the Solution

### 5.1 Run Pipeline

1. Open `Adventureworks_Fabric_Logging` pipeline
2. Click **Run**
3. Monitor execution

### 5.2 Verify Copy Activities

1. Expand **ForEachTable** activity in run view
2. Check each table shows:
   - ✅ Green checkmark for CopyTable
   - ✅ Green checkmark for LogCopyMetrics
3. Click on a LogCopyMetrics activity
4. View notebook snapshot to see metrics logged

### 5.3 Check Logs Created

1. Go to `ControlLakehouse`
2. Click **SQL analytics endpoint**
3. Run query:
   ```sql
   SELECT * FROM CopyActivityLog ORDER BY CreatedDate DESC;
   ```
4. Verify you see rows for each table copied

### 5.4 Check Validation Results

1. In SQL endpoint, run:
   ```sql
   SELECT * FROM BronzeValidationLog ORDER BY ValidationDateTime DESC;
   ```
2. Verify validation ran and shows Pass/Fail status

---

## Step 6: Set Up Monitoring

### 6.1 Create Monitoring Queries

1. In `ControlLakehouse` SQL endpoint
2. Click **New SQL query**
3. Save useful queries from `/sql-queries/monitoring-queries.sql`

### 6.2 Run Monitoring Dashboard (Optional)

1. Open `Monitoring_Dashboard` notebook
2. Run all cells
3. Review output for:
   - Pipeline performance
   - Validation status
   - Error summary

### 6.3 Create Power BI Report (Optional)

1. In Power BI Desktop
2. Connect to ControlLakehouse SQL endpoint
3. Import tables: CopyActivityLog, BronzeValidationLog
4. Create visuals:
   - Line chart: Daily rows processed
   - Card: Success rate %
   - Table: Failed activities
   - Bar chart: Per-table throughput

---

## Verification Checklist

Use this checklist to ensure everything is set up correctly:

### Lakehouses
- [ ] ControlLakehouse created
- [ ] Bronze lakehouse exists
- [ ] Both lakehouse IDs noted

### Notebooks
- [ ] Initialize_Control_Tables created and run successfully
- [ ] Log_Copy_Metrics created with parameter cell
- [ ] Log_Copy_Metrics has ControlLakehouse path configured
- [ ] Bronze_Validation created with both lakehouses attached
- [ ] Bronze_Validation has JDBC configured
- [ ] Bronze_Validation has ControlLakehouse path configured

### Delta Tables
- [ ] CopyActivityLog exists in ControlLakehouse
- [ ] BronzeValidationLog exists in ControlLakehouse
- [ ] ChecksumValidation exists in ControlLakehouse
- [ ] MedallionLayerValidation exists in ControlLakehouse

### Pipeline
- [ ] Adventureworks_Fabric_Logging created
- [ ] Connection IDs updated in JSON
- [ ] Workspace/lakehouse IDs updated
- [ ] Notebook activities reference correct notebook names
- [ ] Pipeline runs without errors

### Functionality
- [ ] Copy activities complete successfully
- [ ] LogCopyMetrics logs data to CopyActivityLog
- [ ] ValidateBronzeLayer runs after ForEach
- [ ] Validation results appear in BronzeValidationLog
- [ ] Can query logs via SQL endpoint

### Monitoring
- [ ] Can view CopyActivityLog data
- [ ] Can view BronzeValidationLog data
- [ ] Monitoring queries saved
- [ ] Dashboard notebook works (if created)

---

## Common Issues and Solutions

### Issue: "Notebook not found" in pipeline
**Solution**: Ensure notebook names match exactly (case-sensitive): `Log_Copy_Metrics`, `Bronze_Validation`

### Issue: CopyActivityLog is empty
**Solution**:
1. Check ControlLakehouse is attached to Log_Copy_Metrics
2. Verify absolute path is configured correctly
3. Check notebook ran successfully in pipeline

### Issue: Validation shows PATH_NOT_FOUND
**Solution**:
1. Ensure Bronze lakehouse attached to Bronze_Validation
2. Verify execution_date parameter is set
3. Check files exist in Bronze: Files/adventureworks/{date}/

### Issue: ProtocolChangedException
**Solution**: Retry logic should handle this automatically. If persists, increase max_retries in notebooks.

### Issue: Data writing to wrong lakehouse
**Solution**: Use absolute ABFS path with workspace and lakehouse IDs instead of relative "Tables/" path

---

## Next Steps

After successful setup:

1. **Schedule Pipeline**: Set up regular execution schedule
2. **Add Alerts**: Configure alerts for failed validations
3. **Extend Validation**: Add checksum validation for financial data
4. **Build Reports**: Create Power BI dashboards
5. **Document**: Add custom tables to validation list
6. **Scale**: Replicate for other pipelines

---

## Support Resources

- [Microsoft Fabric Documentation](https://learn.microsoft.com/fabric/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [PySpark Reference](https://spark.apache.org/docs/latest/api/python/)
- Project README: See main README.md for architecture and features

---

**Setup Time**: Approximately 30-45 minutes
**Difficulty**: Intermediate
**Prerequisites**: Fabric workspace, source database, basic PySpark knowledge
