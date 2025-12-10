# Quick Start Guide: Incremental Loads Setup

This is a condensed, step-by-step guide to get you started with incremental loads from Azure Synapse to Azure SQL Database.

## Prerequisites

- Azure SQL Database (destination)
- Azure Synapse Analytics (source with D365 F&O data)
- Azure Data Factory
- Appropriate permissions on both databases

## 5-Step Setup

### Step 1: Create Control Infrastructure (5 minutes)

Execute this script in **Azure SQL Database**:

```sql
-- Create schemas
CREATE SCHEMA control;
CREATE SCHEMA staging;
GO

-- Create watermark tracking table
CREATE TABLE [control].[WatermarkTable] (
    TableName NVARCHAR(255) PRIMARY KEY,
    SchemaName NVARCHAR(128) NOT NULL,
    WatermarkColumn NVARCHAR(128) NOT NULL,
    WatermarkValue DATETIME2(7) NULL,
    LastLoadDateTime DATETIME2(7) NULL,
    LoadStatus NVARCHAR(50) NULL,
    RowsLoaded BIGINT NULL,
    ErrorMessage NVARCHAR(MAX) NULL
);
GO

-- Register your first table (AssetBook example)
INSERT INTO [control].[WatermarkTable]
VALUES (
    'AssetBook',                    -- TableName
    'dbo',                          -- SchemaName
    'MODIFIEDDATETIME',             -- WatermarkColumn
    '1900-01-01',                   -- Initial WatermarkValue
    NULL,                           -- LastLoadDateTime
    'Not Started',                  -- LoadStatus
    NULL,                           -- RowsLoaded
    NULL                            -- ErrorMessage
);
GO
```

### Step 2: Create Primary Keys Reference (2 minutes)

```sql
-- Create reference table for primary keys
CREATE TABLE [control].[TablePrimaryKeys] (
    SchemaName NVARCHAR(128),
    TableName NVARCHAR(255),
    PrimaryKeyColumns NVARCHAR(MAX),
    PRIMARY KEY (SchemaName, TableName)
);
GO

-- Add primary key for AssetBook (typically RECID for D365 tables)
INSERT INTO [control].[TablePrimaryKeys]
VALUES ('dbo', 'AssetBook', 'RECID');
GO
```

### Step 3: Create Stored Procedures (3 minutes)

```sql
-- Procedure 1: Get primary keys
CREATE PROCEDURE [control].[usp_GetTablePrimaryKeys]
    @SchemaName NVARCHAR(128),
    @TableName NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT PrimaryKeyColumns
    FROM [control].[TablePrimaryKeys]
    WHERE SchemaName = @SchemaName AND TableName = @TableName;
END;
GO

-- Procedure 2: Merge data (simplified version)
CREATE PROCEDURE [control].[usp_MergeIncrementalData]
    @TargetSchema NVARCHAR(128),
    @TargetTable NVARCHAR(255),
    @PrimaryKeyColumns NVARCHAR(MAX),
    @StagingSchema NVARCHAR(128) = 'staging'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @StagingTable NVARCHAR(255) = @TargetTable + '_Stage';

    -- Build match condition
    DECLARE @MatchCondition NVARCHAR(MAX);
    SELECT @MatchCondition = STRING_AGG(
        'target.[' + value + '] = source.[' + value + ']', ' AND '
    ) FROM STRING_SPLIT(@PrimaryKeyColumns, ',');

    -- Build UPDATE SET clause (all columns except PK)
    DECLARE @UpdateSet NVARCHAR(MAX);
    SELECT @UpdateSet = STRING_AGG(
        'target.[' + COLUMN_NAME + '] = source.[' + COLUMN_NAME + ']', ', '
    )
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @StagingSchema
        AND TABLE_NAME = @StagingTable
        AND COLUMN_NAME NOT IN (SELECT value FROM STRING_SPLIT(@PrimaryKeyColumns, ','));

    -- Build INSERT columns and values
    DECLARE @Columns NVARCHAR(MAX), @Values NVARCHAR(MAX);
    SELECT
        @Columns = STRING_AGG('[' + COLUMN_NAME + ']', ', '),
        @Values = STRING_AGG('source.[' + COLUMN_NAME + ']', ', ')
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @StagingSchema AND TABLE_NAME = @StagingTable
    ORDER BY ORDINAL_POSITION;

    -- Execute MERGE
    SET @SQL = '
    MERGE [' + @TargetSchema + '].[' + @TargetTable + '] AS target
    USING [' + @StagingSchema + '].[' + @StagingTable + '] AS source
    ON ' + @MatchCondition + '
    WHEN MATCHED THEN
        UPDATE SET ' + @UpdateSet + '
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (' + @Columns + ')
        VALUES (' + @Values + ');';

    EXEC sp_executesql @SQL;
END;
GO

-- Procedure 3: Update watermark
CREATE PROCEDURE [control].[usp_UpdateWatermark]
    @TableName NVARCHAR(255),
    @SchemaName NVARCHAR(128),
    @WatermarkValue DATETIME2(7),
    @RowsLoaded BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE [control].[WatermarkTable]
    SET WatermarkValue = @WatermarkValue,
        LastLoadDateTime = GETUTCDATE(),
        LoadStatus = 'Success',
        RowsLoaded = @RowsLoaded,
        ErrorMessage = NULL
    WHERE TableName = @TableName AND SchemaName = @SchemaName;
END;
GO

-- Procedure 4: Log errors
CREATE PROCEDURE [control].[usp_LogError]
    @TableName NVARCHAR(255),
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE [control].[WatermarkTable]
    SET LastLoadDateTime = GETUTCDATE(),
        LoadStatus = 'Failed',
        ErrorMessage = @ErrorMessage
    WHERE TableName = @TableName;
END;
GO
```

### Step 4: Perform Initial Full Load (10-60 minutes depending on data size)

Before switching to incremental loads, you need a baseline:

1. **Run your existing full load pipeline** for AssetBook
2. **Set the initial watermark** to current max value:

```sql
-- After full load completes, set watermark to current max
UPDATE [control].[WatermarkTable]
SET WatermarkValue = (
    SELECT MAX(MODIFIEDDATETIME)
    FROM [your_schema].[AssetBook]
),
LoadStatus = 'Initial Load Complete',
LastLoadDateTime = GETUTCDATE()
WHERE TableName = 'AssetBook';
```

### Step 5: Deploy Incremental Pipeline (15 minutes)

Use the `example-incremental-pipeline.json` file provided in this repository as a template.

**Key modifications needed:**

1. Update dataset references to match your environment:
   - `synw_dataverse_dataset` → your Synapse dataset
   - `sqldb_control_dataset` → your Azure SQL dataset
   - `sqldb_staging_dataset` → your Azure SQL dataset with staging schema

2. Update global parameters to match your configuration

3. Create datasets if needed:
   - One for Azure SQL staging tables
   - One for Azure SQL control tables

4. Test with AssetBook first before rolling out to other tables

## Testing Your Setup

### Test 1: Verify No Changes

```sql
-- Check current watermark
SELECT * FROM [control].[WatermarkTable] WHERE TableName = 'AssetBook';

-- Run incremental pipeline
-- Should complete quickly with 0 rows copied
```

### Test 2: Verify Incremental Load

```sql
-- In Synapse (source), update a record
UPDATE [dbo].[AssetBook]
SET [SomeColumn] = 'Test Value'
WHERE RECID = 123456;
-- Note: This may not be possible in read-only Synapse environments

-- Or wait for natural data changes from D365 F&O

-- Run incremental pipeline
-- Should copy only the changed record(s)

-- Verify in Azure SQL Database
SELECT * FROM [control].[WatermarkTable] WHERE TableName = 'AssetBook';
-- RowsLoaded should show number of changed rows
```

### Test 3: Check Monitoring

```sql
-- View load status
SELECT
    TableName,
    LoadStatus,
    LastLoadDateTime,
    RowsLoaded,
    WatermarkValue,
    ErrorMessage
FROM [control].[WatermarkTable]
ORDER BY LastLoadDateTime DESC;
```

## Common D365 F&O Tables and Their Watermark Columns

| Table Name | Primary Key | Watermark Column |
|-----------|-------------|------------------|
| AssetBook | RECID | MODIFIEDDATETIME |
| CustTable | ACCOUNTNUM | MODIFIEDDATETIME |
| VendTable | ACCOUNTNUM | MODIFIEDDATETIME |
| InventTable | ITEMID | MODIFIEDDATETIME |
| SalesTable | SALESID | MODIFIEDDATETIME |
| PurchTable | PURCHID | MODIFIEDDATETIME |

**Note**: Most D365 F&O tables have `MODIFIEDDATETIME` column. Always verify with:

```sql
-- Check if table has MODIFIEDDATETIME
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'YourTableName'
    AND COLUMN_NAME IN ('MODIFIEDDATETIME', 'CREATEDDATETIME', 'RECID');
```

## Adding More Tables

To add additional tables to incremental load:

```sql
-- 1. Register table in watermark table
INSERT INTO [control].[WatermarkTable]
VALUES (
    'CustTable',              -- Your table name
    'dbo',                    -- Schema
    'MODIFIEDDATETIME',       -- Watermark column
    '1900-01-01',             -- Initial value
    NULL, 'Not Started', NULL, NULL
);

-- 2. Register primary key
INSERT INTO [control].[TablePrimaryKeys]
VALUES ('dbo', 'CustTable', 'ACCOUNTNUM');

-- 3. Run initial full load (use existing pipeline)

-- 4. Set initial watermark
UPDATE [control].[WatermarkTable]
SET WatermarkValue = (SELECT MAX(MODIFIEDDATETIME) FROM [dbo].[CustTable]),
    LoadStatus = 'Initial Load Complete',
    LastLoadDateTime = GETUTCDATE()
WHERE TableName = 'CustTable';

-- 5. Create new incremental pipeline or modify existing to accept parameters
```

## Monitoring Queries

### Daily Health Check

```sql
-- Failed loads in last 24 hours
SELECT TableName, LoadStatus, ErrorMessage, LastLoadDateTime
FROM [control].[WatermarkTable]
WHERE LoadStatus = 'Failed'
    OR DATEDIFF(HOUR, LastLoadDateTime, GETUTCDATE()) > 24
ORDER BY LastLoadDateTime DESC;
```

### Load Statistics

```sql
-- Summary of loads
SELECT
    COUNT(*) AS TotalTables,
    SUM(CASE WHEN LoadStatus = 'Success' THEN 1 ELSE 0 END) AS Success,
    SUM(CASE WHEN LoadStatus = 'Failed' THEN 1 ELSE 0 END) AS Failed,
    SUM(RowsLoaded) AS TotalRowsLoaded
FROM [control].[WatermarkTable]
WHERE LastLoadDateTime >= DATEADD(DAY, -1, GETUTCDATE());
```

## Troubleshooting

### Problem: No rows copied but data has changed

**Solution**: Check watermark value
```sql
-- Compare watermark with source
SELECT
    w.WatermarkValue AS LastWatermark,
    (SELECT MAX(MODIFIEDDATETIME) FROM [source].[AssetBook]) AS CurrentMax
FROM [control].[WatermarkTable] w
WHERE TableName = 'AssetBook';

-- If LastWatermark >= CurrentMax, watermark is ahead. Reset if needed:
UPDATE [control].[WatermarkTable]
SET WatermarkValue = '2025-01-01'  -- Use appropriate date
WHERE TableName = 'AssetBook';
```

### Problem: Duplicate key error during merge

**Solution**: Verify primary key configuration
```sql
-- Check registered primary key
SELECT * FROM [control].[TablePrimaryKeys]
WHERE TableName = 'AssetBook';

-- Verify actual primary key in destination
SELECT
    COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE TABLE_NAME = 'AssetBook'
    AND OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1;
```

### Problem: Pipeline timeout

**Solution**: Reduce batch size by using date ranges
```sql
-- Instead of all changes, load in smaller windows
-- Modify pipeline WHERE clause:
WHERE MODIFIEDDATETIME > '@{variables('StartDate')}'
  AND MODIFIEDDATETIME <= '@{variables('EndDate')}'

-- Use ForEach to loop through date ranges (e.g., daily windows)
```

## Pipeline Scheduling Recommendations

| Data Change Frequency | Recommended Schedule |
|----------------------|---------------------|
| High (>1000 rows/hour) | Every 15-30 minutes |
| Medium (100-1000 rows/hour) | Hourly |
| Low (<100 rows/hour) | Every 4-6 hours |
| Very Low | Daily |

**Tip**: Start with hourly loads and adjust based on monitoring data.

## Expected Performance Improvements

Based on typical D365 F&O usage patterns:

| Metric | Full Load | Incremental Load | Improvement |
|--------|-----------|------------------|-------------|
| Execution Time | 30-60 min | 2-5 min | **~90% faster** |
| Data Transfer | 1-10 GB | 10-100 MB | **~99% less** |
| DTU/vCore Usage | High | Low | **~80% reduction** |
| Cost per Load | $$$ | $ | **~95% cheaper** |

*Results vary based on table size and change rate*

## Next Steps After Setup

1. **Enable for more tables** (5-10 at a time)
2. **Set up monitoring alerts** (Azure Monitor/Data Factory)
3. **Create dashboard** (Power BI or Azure Dashboard)
4. **Document table-specific settings** (watermark columns, PKs)
5. **Establish SLAs** (data freshness requirements)
6. **Plan for deletes** (periodic full reconciliation or soft deletes)

## Support and Additional Resources

- **Full Guide**: See `Incremental-Loads-Guide.md` for comprehensive documentation
- **Example Pipeline**: See `example-incremental-pipeline.json` for complete ADF pipeline
- **Microsoft Docs**: [Azure Data Factory Incremental Copy](https://docs.microsoft.com/azure/data-factory/tutorial-incremental-copy-overview)

## Checklist

- [ ] Created control and staging schemas
- [ ] Created WatermarkTable and TablePrimaryKeys tables
- [ ] Created 4 stored procedures
- [ ] Registered AssetBook in control tables
- [ ] Performed initial full load
- [ ] Set initial watermark value
- [ ] Created/modified ADF datasets for staging
- [ ] Deployed incremental pipeline
- [ ] Tested with no changes (0 rows)
- [ ] Tested with changes (>0 rows)
- [ ] Verified data accuracy in destination
- [ ] Set up monitoring queries
- [ ] Scheduled pipeline

---

**Estimated Total Setup Time**: 1-2 hours (excluding initial full load)

**Difficulty Level**: Intermediate

**Prerequisites Knowledge**:
- Basic SQL
- Azure Data Factory
- Azure SQL Database administration
