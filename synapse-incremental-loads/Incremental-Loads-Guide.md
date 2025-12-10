# Incremental Loads from Azure Synapse Analytics to Azure SQL Database

## Executive Summary

This guide provides recommendations for implementing incremental loads from Azure Synapse Analytics to Azure SQL Database for Microsoft Dynamics 365 Finance & Operations (F&O) data. The current implementation uses full loads (drop and recreate), which can be optimized using incremental load patterns.

## Table of Contents

1. [Current State Analysis](#current-state-analysis)
2. [Incremental Load Strategies](#incremental-load-strategies)
3. [Recommended Approach](#recommended-approach)
4. [Implementation Guide](#implementation-guide)
5. [Pipeline Modifications](#pipeline-modifications)
6. [Metadata Management](#metadata-management)
7. [Best Practices](#best-practices)
8. [Performance Considerations](#performance-considerations)

---

## Current State Analysis

### Current Pipeline Behavior

Your existing pipeline performs **full loads**:

```sql
-- Pre-copy script drops the entire table
DROP TABLE IF EXISTS [schema].[table_name]

-- Then recreates and loads all data
-- tableOption: "autoCreate"
```

### Limitations of Full Loads

- **High Data Transfer Costs**: Transferring entire tables repeatedly
- **Increased Load Time**: Processing all rows regardless of changes
- **Resource Intensive**: High CPU, memory, and I/O consumption
- **Longer Downtime**: Tables unavailable during drop/recreate
- **Network Bandwidth**: Transferring unchanged data

---

## Incremental Load Strategies

### 1. Watermark-Based (High Watermark)

**Best for**: Tables with reliable timestamp or sequential ID columns

**How it works**:
- Track the last processed timestamp/ID (watermark)
- Only load records where timestamp/ID > last watermark
- Update watermark after successful load

**Pros**:
- Simple to implement
- Works with most tables
- Low overhead

**Cons**:
- Doesn't capture deletes
- Requires timestamp or sequential column
- Updates to old records might be missed

### 2. Change Tracking (SQL Server Native)

**Best for**: Azure SQL environments with Change Tracking enabled

**How it works**:
- SQL Server tracks changes (INSERT, UPDATE, DELETE)
- Query change tracking tables for modified records
- Sync changes incrementally

**Pros**:
- Captures all change types (INSERT, UPDATE, DELETE)
- Built-in SQL Server feature
- Minimal overhead

**Cons**:
- Requires Change Tracking to be enabled on source
- Retention period limitations
- May not be available in Synapse dedicated pool

### 3. Change Data Capture (CDC)

**Best for**: Enterprise scenarios requiring full audit trail

**How it works**:
- Captures all DML operations from transaction log
- Provides before/after values
- Complete change history

**Pros**:
- Complete audit trail
- Captures all changes
- Historical analysis capability

**Cons**:
- Higher overhead
- More complex implementation
- May not be available in all Synapse configurations

### 4. Delta Lake Pattern

**Best for**: Large-scale data lake architectures

**How it works**:
- Store data in Delta Lake format
- Use MERGE operations
- Time travel and versioning capabilities

**Pros**:
- ACID transactions
- Schema evolution
- Time travel

**Cons**:
- Requires Delta Lake infrastructure
- Additional complexity

---

## Recommended Approach

### Primary Recommendation: **Watermark-Based Incremental Loads**

For your D365 F&O scenario, I recommend the **Watermark (High Watermark) approach** because:

1. **D365 F&O Tables** typically have reliable audit columns:
   - `MODIFIEDDATETIME` - Last modified timestamp
   - `RECID` - Sequential record identifier
   - `CREATEDDATETIME` - Creation timestamp

2. **Simpler Implementation**: Minimal changes to existing pipelines

3. **Cost Effective**: Reduces data transfer and processing

4. **Proven Pattern**: Well-established in Azure Data Factory

### Strategy Overview

```
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────┐
│ Azure Synapse   │      │  Watermark       │      │  Azure SQL DB   │
│  (Source)       │─────>│  Tracking Table  │─────>│  (Destination)  │
│  D365 F&O Data  │      │  Last Load Time  │      │  Target Tables  │
└─────────────────┘      └──────────────────┘      └─────────────────┘
         │                        │                         │
         │                        │                         │
         v                        v                         v
   Get new/changed    Update watermark          MERGE/Upsert
   records only       after success             changed data
```

---

## Implementation Guide

### Step 1: Create Watermark Control Table

Create a control table in Azure SQL Database to track the last loaded timestamp for each table:

```sql
-- Execute in Azure SQL Database
CREATE TABLE [control].[WatermarkTable] (
    TableName NVARCHAR(255) PRIMARY KEY,
    SchemaName NVARCHAR(128) NOT NULL,
    WatermarkColumn NVARCHAR(128) NOT NULL,
    WatermarkValue DATETIME2(7) NULL,
    LastLoadDateTime DATETIME2(7) NULL,
    LoadStatus NVARCHAR(50) NULL,
    RowsLoaded BIGINT NULL,
    ErrorMessage NVARCHAR(MAX) NULL,
    CONSTRAINT UQ_Schema_Table UNIQUE (SchemaName, TableName)
);

-- Initialize with your F&O tables
-- Example for AssetBook table
INSERT INTO [control].[WatermarkTable]
    (TableName, SchemaName, WatermarkColumn, WatermarkValue, LastLoadDateTime, LoadStatus)
VALUES
    ('AssetBook', 'dbo', 'MODIFIEDDATETIME', '1900-01-01 00:00:00.0000000', NULL, 'Not Started');

-- Add more tables as needed
INSERT INTO [control].[WatermarkTable]
    (TableName, SchemaName, WatermarkColumn, WatermarkValue, LoadStatus)
VALUES
    ('CustTable', 'dbo', 'MODIFIEDDATETIME', '1900-01-01 00:00:00.0000000', 'Not Started'),
    ('VendTable', 'dbo', 'MODIFIEDDATETIME', '1900-01-01 00:00:00.0000000', 'Not Started');
```

### Step 2: Create Staging Tables in Azure SQL DB

Create staging tables to hold incremental data before merging:

```sql
-- Option A: Auto-create staging schema
CREATE SCHEMA [staging];

-- Staging tables will be auto-created or you can pre-create them
-- Naming convention: staging.{TableName}_Stage
```

### Step 3: Create Merge Stored Procedure (Upsert Logic)

Create a generic stored procedure to merge staging data into target tables:

```sql
CREATE PROCEDURE [control].[usp_MergeIncrementalData]
    @TargetSchema NVARCHAR(128),
    @TargetTable NVARCHAR(255),
    @PrimaryKeyColumns NVARCHAR(MAX), -- Comma-separated list
    @StagingSchema NVARCHAR(128) = 'staging'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @MergeSql NVARCHAR(MAX);
    DECLARE @StagingTable NVARCHAR(255) = @TargetTable + '_Stage';
    DECLARE @MatchCondition NVARCHAR(MAX);
    DECLARE @UpdateSet NVARCHAR(MAX);
    DECLARE @InsertColumns NVARCHAR(MAX);
    DECLARE @InsertValues NVARCHAR(MAX);

    -- Build match condition for primary keys
    SELECT @MatchCondition = STRING_AGG(
        'target.[' + value + '] = source.[' + value + ']',
        ' AND '
    )
    FROM STRING_SPLIT(@PrimaryKeyColumns, ',');

    -- Get all columns except primary keys for UPDATE
    SELECT @UpdateSet = STRING_AGG(
        'target.[' + COLUMN_NAME + '] = source.[' + COLUMN_NAME + ']',
        ', '
    )
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @StagingSchema
        AND TABLE_NAME = @StagingTable
        AND COLUMN_NAME NOT IN (SELECT value FROM STRING_SPLIT(@PrimaryKeyColumns, ','));

    -- Get all columns for INSERT
    SELECT @InsertColumns = STRING_AGG('[' + COLUMN_NAME + ']', ', '),
           @InsertValues = STRING_AGG('source.[' + COLUMN_NAME + ']', ', ')
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @StagingSchema
        AND TABLE_NAME = @StagingTable
    ORDER BY ORDINAL_POSITION;

    -- Build MERGE statement
    SET @MergeSql = '
    MERGE [' + @TargetSchema + '].[' + @TargetTable + '] AS target
    USING [' + @StagingSchema + '].[' + @StagingTable + '] AS source
    ON ' + @MatchCondition + '
    WHEN MATCHED THEN
        UPDATE SET ' + @UpdateSet + '
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (' + @InsertColumns + ')
        VALUES (' + @InsertValues + ');';

    -- Execute merge
    EXEC sp_executesql @MergeSql;

    -- Return rows affected
    SELECT @@ROWCOUNT AS RowsAffected;
END;
```

### Step 4: Create Metadata Stored Procedure for Primary Keys

Create a stored procedure to get primary key information:

```sql
CREATE PROCEDURE [control].[usp_GetTablePrimaryKeys]
    @SchemaName NVARCHAR(128),
    @TableName NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT STRING_AGG(COLUMN_NAME, ',') AS PrimaryKeyColumns
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
        AND TABLE_SCHEMA = @SchemaName
        AND TABLE_NAME = @TableName;
END;
```

---

## Pipeline Modifications

### Modified Pipeline JSON Structure

Here's the enhanced pipeline with incremental load capabilities:

```json
{
    "name": "d365fo_assetbook_incremental",
    "properties": {
        "activities": [
            {
                "name": "get_old_watermark",
                "description": "Get the last watermark value from control table",
                "type": "Lookup",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "source": {
                        "type": "AzureSqlSource",
                        "sqlReaderQuery": {
                            "value": "SELECT TableName, WatermarkColumn, WatermarkValue FROM [control].[WatermarkTable] WHERE TableName = '@{pipeline().parameters.input_asis_table_name}' AND SchemaName = '@{pipeline().parameters.input_table_schema}'",
                            "type": "Expression"
                        },
                        "queryTimeout": "02:00:00"
                    },
                    "dataset": {
                        "referenceName": "sqldb_dataset",
                        "type": "DatasetReference"
                    },
                    "firstRowOnly": true
                }
            },
            {
                "name": "get_new_watermark",
                "description": "Get the maximum watermark value from source",
                "type": "Lookup",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "source": {
                        "type": "SqlDWSource",
                        "sqlReaderQuery": {
                            "value": "SELECT ISNULL(MAX(MODIFIEDDATETIME), '1900-01-01') AS NewWatermarkValue FROM [@{pipeline().parameters.input_table_schema}].[@{pipeline().parameters.input_asis_table_name}]",
                            "type": "Expression"
                        },
                        "queryTimeout": "02:00:00",
                        "partitionOption": "None"
                    },
                    "dataset": {
                        "referenceName": "synw_dataverse_dataset",
                        "type": "DatasetReference"
                    },
                    "firstRowOnly": true
                }
            },
            {
                "name": "get_metadata",
                "description": "Get metadata about table schema and columns",
                "type": "Lookup",
                "dependsOn": [
                    {
                        "activity": "get_old_watermark",
                        "dependencyConditions": ["Succeeded"]
                    },
                    {
                        "activity": "get_new_watermark",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "source": {
                        "type": "SqlDWSource",
                        "sqlReaderQuery": {
                            "value": "SELECT TABLE_NAME AS metadata_table_name, STUFF((SELECT ', [' + CAST(COLUMN_NAME AS VARCHAR(100)) +']' [text()] FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = t.TABLE_NAME and TABLE_SCHEMA = t.TABLE_SCHEMA order by Ordinal_position Asc FOR XML PATH(''), TYPE).value('.','NVARCHAR(MAX)'),1,2,' ') AS metadata_column_name FROM INFORMATION_SCHEMA.COLUMNS t WHERE TABLE_SCHEMA = '@{pipeline().parameters.input_table_schema}' AND TABLE_NAME = '@{pipeline().parameters.input_asis_table_name}' GROUP BY TABLE_NAME, TABLE_SCHEMA",
                            "type": "Expression"
                        },
                        "queryTimeout": "02:00:00",
                        "partitionOption": "None"
                    },
                    "dataset": {
                        "referenceName": "synw_dataverse_dataset",
                        "type": "DatasetReference"
                    },
                    "firstRowOnly": true
                }
            },
            {
                "name": "copy_incremental_data",
                "description": "Copy only changed data to staging table",
                "type": "Copy",
                "dependsOn": [
                    {
                        "activity": "get_metadata",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "source": {
                        "type": "SqlDWSource",
                        "sqlReaderQuery": {
                            "value": "SELECT @{activity('get_metadata').output.firstRow.metadata_column_name} FROM @{pipeline().parameters.input_table_schema}.@{pipeline().parameters.input_asis_table_name} WHERE @{activity('get_old_watermark').output.firstRow.WatermarkColumn} > '@{activity('get_old_watermark').output.firstRow.WatermarkValue}' AND @{activity('get_old_watermark').output.firstRow.WatermarkColumn} <= '@{activity('get_new_watermark').output.firstRow.NewWatermarkValue}'",
                            "type": "Expression"
                        },
                        "queryTimeout": "02:00:00",
                        "partitionOption": "None"
                    },
                    "sink": {
                        "type": "AzureSqlSink",
                        "preCopyScript": {
                            "value": "IF OBJECT_ID('[staging].[@{pipeline().parameters.input_asis_table_name}_Stage]', 'U') IS NOT NULL TRUNCATE TABLE [staging].[@{pipeline().parameters.input_asis_table_name}_Stage]",
                            "type": "Expression"
                        },
                        "writeBehavior": "insert",
                        "sqlWriterUseTableLock": false,
                        "tableOption": "autoCreate",
                        "disableMetricsCollection": false
                    },
                    "enableStaging": false,
                    "translator": {
                        "type": "TabularTranslator",
                        "typeConversion": true,
                        "typeConversionSettings": {
                            "allowDataTruncation": true,
                            "treatBooleanAsNumber": false
                        }
                    }
                },
                "inputs": [
                    {
                        "referenceName": "synw_dataverse_dataset",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "sqldb_staging_dataset",
                        "type": "DatasetReference"
                    }
                ]
            },
            {
                "name": "get_primary_keys",
                "description": "Get primary key columns for merge operation",
                "type": "Lookup",
                "dependsOn": [
                    {
                        "activity": "copy_incremental_data",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "source": {
                        "type": "AzureSqlSource",
                        "sqlReaderStoredProcedureName": "[control].[usp_GetTablePrimaryKeys]",
                        "storedProcedureParameters": {
                            "SchemaName": {
                                "type": "String",
                                "value": {
                                    "value": "@pipeline().globalParameters.gp_synw_schema_DSA_D365FO",
                                    "type": "Expression"
                                }
                            },
                            "TableName": {
                                "type": "String",
                                "value": {
                                    "value": "@pipeline().parameters.input_asis_table_name",
                                    "type": "Expression"
                                }
                            }
                        },
                        "queryTimeout": "02:00:00"
                    },
                    "dataset": {
                        "referenceName": "sqldb_dataset",
                        "type": "DatasetReference"
                    },
                    "firstRowOnly": true
                }
            },
            {
                "name": "merge_to_target",
                "description": "Merge staging data into target table",
                "type": "SqlServerStoredProcedure",
                "dependsOn": [
                    {
                        "activity": "get_primary_keys",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "storedProcedureName": "[control].[usp_MergeIncrementalData]",
                    "storedProcedureParameters": {
                        "TargetSchema": {
                            "value": {
                                "value": "@pipeline().globalParameters.gp_synw_schema_DSA_D365FO",
                                "type": "Expression"
                            },
                            "type": "String"
                        },
                        "TargetTable": {
                            "value": {
                                "value": "@pipeline().parameters.input_asis_table_name",
                                "type": "Expression"
                            },
                            "type": "String"
                        },
                        "PrimaryKeyColumns": {
                            "value": {
                                "value": "@activity('get_primary_keys').output.firstRow.PrimaryKeyColumns",
                                "type": "Expression"
                            },
                            "type": "String"
                        },
                        "StagingSchema": {
                            "value": "staging",
                            "type": "String"
                        }
                    }
                },
                "linkedServiceName": {
                    "referenceName": "sqldb_linked_service",
                    "type": "LinkedServiceReference"
                }
            },
            {
                "name": "update_watermark",
                "description": "Update watermark value after successful load",
                "type": "SqlServerStoredProcedure",
                "dependsOn": [
                    {
                        "activity": "merge_to_target",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "storedProcedureName": "[control].[usp_UpdateWatermark]",
                    "storedProcedureParameters": {
                        "TableName": {
                            "value": {
                                "value": "@pipeline().parameters.input_asis_table_name",
                                "type": "Expression"
                            },
                            "type": "String"
                        },
                        "SchemaName": {
                            "value": {
                                "value": "@pipeline().parameters.input_table_schema",
                                "type": "Expression"
                            },
                            "type": "String"
                        },
                        "WatermarkValue": {
                            "value": {
                                "value": "@activity('get_new_watermark').output.firstRow.NewWatermarkValue",
                                "type": "Expression"
                            },
                            "type": "DateTime"
                        },
                        "RowsLoaded": {
                            "value": {
                                "value": "@activity('copy_incremental_data').output.rowsCopied",
                                "type": "Expression"
                            },
                            "type": "Int64"
                        }
                    }
                },
                "linkedServiceName": {
                    "referenceName": "sqldb_linked_service",
                    "type": "LinkedServiceReference"
                }
            },
            {
                "name": "log_error",
                "description": "Log error to watermark table",
                "type": "SqlServerStoredProcedure",
                "dependsOn": [
                    {
                        "activity": "merge_to_target",
                        "dependencyConditions": ["Failed"]
                    }
                ],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "storedProcedureName": "[control].[usp_LogError]",
                    "storedProcedureParameters": {
                        "TableName": {
                            "value": {
                                "value": "@pipeline().parameters.input_asis_table_name",
                                "type": "Expression"
                            },
                            "type": "String"
                        },
                        "ErrorMessage": {
                            "value": {
                                "value": "@activity('merge_to_target').output.errors[0].Message",
                                "type": "Expression"
                            },
                            "type": "String"
                        }
                    }
                },
                "linkedServiceName": {
                    "referenceName": "sqldb_linked_service",
                    "type": "LinkedServiceReference"
                }
            }
        ],
        "parameters": {
            "input_table_schema": {
                "type": "string",
                "defaultValue": "dbo"
            },
            "input_asis_table_name": {
                "type": "string",
                "defaultValue": "AssetBook"
            }
        },
        "folder": {
            "name": "D365FO_Incremental/a-h_group"
        },
        "annotations": []
    }
}
```

### Additional Stored Procedures Needed

```sql
-- Stored procedure to update watermark
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
    WHERE TableName = @TableName
        AND SchemaName = @SchemaName;
END;

-- Stored procedure to log errors
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
```

---

## Metadata Management

### Understanding D365 F&O Audit Columns

Most D365 F&O tables include these system columns:

| Column Name | Data Type | Purpose |
|------------|-----------|---------|
| RECID | BIGINT | Unique record identifier (auto-increment) |
| MODIFIEDDATETIME | DATETIME2 | Last modification timestamp |
| CREATEDDATETIME | DATETIME2 | Creation timestamp |
| MODIFIEDBY | NVARCHAR | User who modified |
| CREATEDBY | NVARCHAR | User who created |

**Recommended Watermark Column**: `MODIFIEDDATETIME`

### Handling Tables Without Timestamps

For tables without `MODIFIEDDATETIME`:

1. **Option 1**: Use `RECID` as watermark
```sql
WHERE RECID > @LastRecId AND RECID <= @NewRecId
```

2. **Option 2**: Add custom timestamp column (if possible)

3. **Option 3**: Continue with full load for those specific tables

---

## Best Practices

### 1. Initial Full Load Strategy

Before enabling incremental loads:

```sql
-- Step 1: Perform initial full load using existing pipeline
-- Step 2: Set initial watermark to current max value
UPDATE [control].[WatermarkTable]
SET WatermarkValue = (
    SELECT MAX(MODIFIEDDATETIME)
    FROM [source_schema].[TableName]
),
LoadStatus = 'Initial Load Complete',
LastLoadDateTime = GETUTCDATE()
WHERE TableName = 'TableName';

-- Step 3: Switch to incremental pipeline
```

### 2. Handle Primary Key Determination

For D365 F&O tables, common primary keys:

- **RECID**: Universal unique identifier
- **Composite Keys**: Multiple columns (check `INFORMATION_SCHEMA.KEY_COLUMN_USAGE`)

Create a reference table for tables with non-standard keys:

```sql
CREATE TABLE [control].[TablePrimaryKeys] (
    SchemaName NVARCHAR(128),
    TableName NVARCHAR(255),
    PrimaryKeyColumns NVARCHAR(MAX), -- Comma-separated
    PRIMARY KEY (SchemaName, TableName)
);

-- Populate for D365 tables
INSERT INTO [control].[TablePrimaryKeys] VALUES
('dbo', 'AssetBook', 'RECID'),
('dbo', 'CustTable', 'ACCOUNTNUM'),
('dbo', 'VendTable', 'ACCOUNTNUM');
```

### 3. Monitoring and Alerting

Create a monitoring view:

```sql
CREATE VIEW [control].[vw_LoadMonitoring] AS
SELECT
    TableName,
    SchemaName,
    LoadStatus,
    LastLoadDateTime,
    RowsLoaded,
    DATEDIFF(HOUR, LastLoadDateTime, GETUTCDATE()) AS HoursSinceLastLoad,
    WatermarkValue,
    ErrorMessage
FROM [control].[WatermarkTable];

-- Query for failed loads
SELECT * FROM [control].[vw_LoadMonitoring]
WHERE LoadStatus = 'Failed'
ORDER BY LastLoadDateTime DESC;

-- Query for stale data (not loaded in 24 hours)
SELECT * FROM [control].[vw_LoadMonitoring]
WHERE HoursSinceLastLoad > 24
ORDER BY HoursSinceLastLoad DESC;
```

### 4. Error Handling

Add retry logic and notifications:

```json
{
    "activities": [
        {
            "name": "merge_to_target",
            "policy": {
                "timeout": "0.12:00:00",
                "retry": 3,
                "retryIntervalInSeconds": 30
            }
        }
    ]
}
```

### 5. Transaction Safety

Ensure ACID properties:

```sql
-- In merge stored procedure, add transaction handling
ALTER PROCEDURE [control].[usp_MergeIncrementalData]
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- MERGE logic here

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH
END;
```

### 6. Handling Deletes

Incremental loads with watermarks don't capture deletes. Options:

**Option A**: Periodic full reconciliation
```sql
-- Schedule weekly full load to catch deletes
-- Compare source vs target and remove orphaned records
```

**Option B**: Soft deletes
```sql
-- If source supports soft delete column (e.g., IsDeleted)
WHERE MODIFIEDDATETIME > @Watermark OR IsDeleted = 1
```

**Option C**: Use Change Tracking (if available)
```sql
-- Leverage SQL Server Change Tracking
SELECT * FROM CHANGETABLE(
    CHANGES [schema].[table],
    @last_sync_version
) AS CT
```

### 7. Partition Strategy for Large Tables

For very large tables (>10M rows), consider partitioning:

```sql
-- Copy in batches using dynamic ranges
WHERE MODIFIEDDATETIME > '@{variables('StartDate')}'
  AND MODIFIEDDATETIME <= '@{variables('EndDate')}'

-- Use ForEach to loop through date ranges
```

---

## Performance Considerations

### 1. Indexing Strategy

**Source (Synapse)**:
```sql
-- Create indexes on watermark columns
CREATE INDEX IX_TableName_ModifiedDateTime
ON [schema].[TableName] (MODIFIEDDATETIME)
INCLUDE (RECID);
```

**Destination (Azure SQL DB)**:
```sql
-- Ensure primary key indexes exist
-- Add covering indexes for common queries
CREATE INDEX IX_TableName_ModifiedDateTime
ON [schema].[TableName] (MODIFIEDDATETIME);
```

### 2. Staging Table Optimization

```sql
-- Use clustered columnstore for large staging tables
CREATE TABLE [staging].[LargeTable_Stage] (
    -- columns
) WITH (CLUSTERED COLUMNSTORE INDEX);

-- Use heap for small staging tables
CREATE TABLE [staging].[SmallTable_Stage] (
    -- columns
) WITH (HEAP);
```

### 3. Parallel Processing

Modify pipeline to process multiple tables in parallel:

```json
{
    "activities": [
        {
            "name": "ForEachTable",
            "type": "ForEach",
            "typeProperties": {
                "items": "@pipeline().parameters.TableList",
                "isSequential": false,
                "batchCount": 10,
                "activities": [
                    {
                        "name": "IncrementalLoadActivity",
                        "type": "ExecutePipeline"
                    }
                ]
            }
        }
    ]
}
```

### 4. PolyBase / COPY Command

For large incremental batches, use PolyBase or COPY command:

```sql
-- In Synapse, use COPY for better performance
COPY INTO [staging].[TableName]
FROM 'https://...'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY = 'Managed Identity')
);
```

### 5. Query Optimization

```sql
-- Use NOLOCK for reading watermarks (acceptable for this use case)
SELECT WatermarkValue
FROM [control].[WatermarkTable] WITH (NOLOCK)
WHERE TableName = 'AssetBook';

-- Use query hints for large merges
MERGE [target] WITH (TABLOCK)
USING [staging] ON ...
OPTION (MAXDOP 4);
```

---

## Migration Plan

### Phase 1: Setup (Week 1)

1. Create control schema and tables
2. Create stored procedures
3. Test with one table (AssetBook)
4. Validate data accuracy

### Phase 2: Pilot (Week 2-3)

1. Enable for 10-20 tables
2. Monitor performance and errors
3. Adjust batch sizes and schedules
4. Document lessons learned

### Phase 3: Full Rollout (Week 4-6)

1. Convert remaining pipelines
2. Establish monitoring dashboards
3. Create runbook documentation
4. Train operations team

### Phase 4: Optimization (Ongoing)

1. Fine-tune scheduling
2. Optimize indexes
3. Implement partitioning for large tables
4. Review and adjust retention policies

---

## Monitoring Queries

### Daily Health Check

```sql
-- Tables with recent failures
SELECT
    TableName,
    LoadStatus,
    LastLoadDateTime,
    RowsLoaded,
    ErrorMessage
FROM [control].[WatermarkTable]
WHERE LoadStatus = 'Failed'
    OR DATEDIFF(HOUR, LastLoadDateTime, GETUTCDATE()) > 24
ORDER BY LastLoadDateTime DESC;

-- Load statistics (last 24 hours)
SELECT
    COUNT(*) AS TotalTables,
    SUM(CASE WHEN LoadStatus = 'Success' THEN 1 ELSE 0 END) AS SuccessfulLoads,
    SUM(CASE WHEN LoadStatus = 'Failed' THEN 1 ELSE 0 END) AS FailedLoads,
    SUM(RowsLoaded) AS TotalRowsLoaded,
    AVG(RowsLoaded) AS AvgRowsPerTable
FROM [control].[WatermarkTable]
WHERE LastLoadDateTime >= DATEADD(HOUR, -24, GETUTCDATE());
```

### Weekly Performance Review

```sql
-- Tables with most changes
SELECT TOP 20
    TableName,
    AVG(RowsLoaded) AS AvgRowsPerLoad,
    SUM(RowsLoaded) AS TotalRowsLastWeek,
    COUNT(*) AS LoadCount
FROM [control].[WatermarkTable]
WHERE LastLoadDateTime >= DATEADD(DAY, -7, GETUTCDATE())
    AND LoadStatus = 'Success'
GROUP BY TableName
ORDER BY TotalRowsLastWeek DESC;
```

---

## Troubleshooting Guide

### Issue: No rows copied

**Cause**: Watermark value ahead of source data
**Solution**:
```sql
-- Reset watermark to earlier date
UPDATE [control].[WatermarkTable]
SET WatermarkValue = DATEADD(DAY, -7, GETUTCDATE())
WHERE TableName = 'YourTable';
```

### Issue: Duplicate key violations

**Cause**: Incorrect primary key definition
**Solution**:
```sql
-- Verify primary keys
SELECT * FROM [control].[usp_GetTablePrimaryKeys]
WHERE TableName = 'YourTable';

-- Update if needed
UPDATE [control].[TablePrimaryKeys]
SET PrimaryKeyColumns = 'RECID'
WHERE TableName = 'YourTable';
```

### Issue: Merge timeout

**Cause**: Large batch size
**Solution**: Implement partitioning or reduce batch window

---

## Cost Optimization

### Data Transfer Costs

**Before (Full Load)**:
- 1TB table loaded daily = 30TB/month transfer
- Cost: ~$1,500/month (at $0.05/GB)

**After (Incremental Load)**:
- 1% daily change = 10GB/day = 300GB/month
- Cost: ~$15/month
- **Savings: ~99%**

### Compute Costs

- **Reduced DTU/vCore usage**: Shorter running pipelines
- **Lower Synapse costs**: Less data scanned
- **Smaller staging**: Reduced storage costs

---

## Summary and Next Steps

### Key Benefits

1. **Performance**: 10-100x faster loads for tables with <10% daily changes
2. **Cost**: 90-99% reduction in data transfer costs
3. **Freshness**: More frequent load windows possible
4. **Resource Utilization**: Lower compute and storage footprint

### Recommended Next Steps

1. **Start Small**: Implement for AssetBook table first
2. **Measure**: Compare before/after metrics
3. **Expand**: Gradually roll out to more tables
4. **Monitor**: Establish dashboards and alerts
5. **Optimize**: Fine-tune based on actual patterns

### Success Metrics

Track these KPIs:
- Pipeline execution time (before vs after)
- Data transfer volume (GB)
- Pipeline success rate (%)
- Data freshness (lag time)
- Cost per pipeline run

---

## Additional Resources

### D365 F&O Data Integration
- [Microsoft Dynamics 365 Data Entities](https://docs.microsoft.com/dynamics365/)
- [Azure Synapse Link for Dynamics](https://docs.microsoft.com/azure/synapse-analytics/)

### Azure Data Factory Patterns
- [ADF Incremental Copy Pattern](https://docs.microsoft.com/azure/data-factory/tutorial-incremental-copy-overview)
- [ADF Best Practices](https://docs.microsoft.com/azure/data-factory/concepts-pipelines-activities)

### SQL Optimization
- [Azure SQL Performance Tuning](https://docs.microsoft.com/azure/sql-database/sql-database-performance-guidance)
- [Synapse Dedicated Pool Best Practices](https://docs.microsoft.com/azure/synapse-analytics/sql-data-warehouse/)

---

## Appendix: Complete Setup Script

```sql
-- ============================================================================
-- Complete Setup Script for Incremental Loads
-- Execute this script in Azure SQL Database
-- ============================================================================

-- Create control schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'control')
BEGIN
    EXEC('CREATE SCHEMA control');
END
GO

-- Create staging schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
BEGIN
    EXEC('CREATE SCHEMA staging');
END
GO

-- Create watermark table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'WatermarkTable' AND schema_id = SCHEMA_ID('control'))
BEGIN
    CREATE TABLE [control].[WatermarkTable] (
        TableName NVARCHAR(255) PRIMARY KEY,
        SchemaName NVARCHAR(128) NOT NULL,
        WatermarkColumn NVARCHAR(128) NOT NULL,
        WatermarkValue DATETIME2(7) NULL,
        LastLoadDateTime DATETIME2(7) NULL,
        LoadStatus NVARCHAR(50) NULL,
        RowsLoaded BIGINT NULL,
        ErrorMessage NVARCHAR(MAX) NULL,
        CONSTRAINT UQ_Schema_Table UNIQUE (SchemaName, TableName)
    );
END
GO

-- Create primary keys reference table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TablePrimaryKeys' AND schema_id = SCHEMA_ID('control'))
BEGIN
    CREATE TABLE [control].[TablePrimaryKeys] (
        SchemaName NVARCHAR(128),
        TableName NVARCHAR(255),
        PrimaryKeyColumns NVARCHAR(MAX),
        PRIMARY KEY (SchemaName, TableName)
    );
END
GO

-- Create merge stored procedure
CREATE OR ALTER PROCEDURE [control].[usp_MergeIncrementalData]
    @TargetSchema NVARCHAR(128),
    @TargetTable NVARCHAR(255),
    @PrimaryKeyColumns NVARCHAR(MAX),
    @StagingSchema NVARCHAR(128) = 'staging'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @MergeSql NVARCHAR(MAX);
    DECLARE @StagingTable NVARCHAR(255) = @TargetTable + '_Stage';
    DECLARE @MatchCondition NVARCHAR(MAX);
    DECLARE @UpdateSet NVARCHAR(MAX);
    DECLARE @InsertColumns NVARCHAR(MAX);
    DECLARE @InsertValues NVARCHAR(MAX);

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Build match condition
        SELECT @MatchCondition = STRING_AGG(
            'target.[' + value + '] = source.[' + value + ']',
            ' AND '
        )
        FROM STRING_SPLIT(@PrimaryKeyColumns, ',');

        -- Build UPDATE SET
        SELECT @UpdateSet = STRING_AGG(
            'target.[' + COLUMN_NAME + '] = source.[' + COLUMN_NAME + ']',
            ', '
        )
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = @StagingSchema
            AND TABLE_NAME = @StagingTable
            AND COLUMN_NAME NOT IN (SELECT value FROM STRING_SPLIT(@PrimaryKeyColumns, ','));

        -- Build INSERT columns and values
        SELECT @InsertColumns = STRING_AGG('[' + COLUMN_NAME + ']', ', '),
               @InsertValues = STRING_AGG('source.[' + COLUMN_NAME + ']', ', ')
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = @StagingSchema
            AND TABLE_NAME = @StagingTable
        ORDER BY ORDINAL_POSITION;

        -- Build and execute MERGE
        SET @MergeSql = '
        MERGE [' + @TargetSchema + '].[' + @TargetTable + '] AS target
        USING [' + @StagingSchema + '].[' + @StagingTable + '] AS source
        ON ' + @MatchCondition + '
        WHEN MATCHED THEN
            UPDATE SET ' + @UpdateSet + '
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (' + @InsertColumns + ')
            VALUES (' + @InsertValues + ');';

        EXEC sp_executesql @MergeSql;

        COMMIT TRANSACTION;
        SELECT @@ROWCOUNT AS RowsAffected;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO

-- Create get primary keys procedure
CREATE OR ALTER PROCEDURE [control].[usp_GetTablePrimaryKeys]
    @SchemaName NVARCHAR(128),
    @TableName NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    -- First check reference table
    IF EXISTS (SELECT 1 FROM [control].[TablePrimaryKeys]
               WHERE SchemaName = @SchemaName AND TableName = @TableName)
    BEGIN
        SELECT PrimaryKeyColumns
        FROM [control].[TablePrimaryKeys]
        WHERE SchemaName = @SchemaName AND TableName = @TableName;
    END
    ELSE
    BEGIN
        -- Auto-detect from INFORMATION_SCHEMA
        SELECT STRING_AGG(COLUMN_NAME, ',') AS PrimaryKeyColumns
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
            AND TABLE_SCHEMA = @SchemaName
            AND TABLE_NAME = @TableName;
    END
END;
GO

-- Create update watermark procedure
CREATE OR ALTER PROCEDURE [control].[usp_UpdateWatermark]
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
    WHERE TableName = @TableName
        AND SchemaName = @SchemaName;
END;
GO

-- Create log error procedure
CREATE OR ALTER PROCEDURE [control].[usp_LogError]
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

-- Create monitoring view
CREATE OR ALTER VIEW [control].[vw_LoadMonitoring] AS
SELECT
    TableName,
    SchemaName,
    LoadStatus,
    LastLoadDateTime,
    RowsLoaded,
    DATEDIFF(HOUR, LastLoadDateTime, GETUTCDATE()) AS HoursSinceLastLoad,
    WatermarkValue,
    ErrorMessage
FROM [control].[WatermarkTable];
GO

PRINT 'Incremental load framework setup complete!';
PRINT 'Next steps:';
PRINT '1. Populate [control].[WatermarkTable] with your tables';
PRINT '2. Optionally populate [control].[TablePrimaryKeys] for tables with non-standard keys';
PRINT '3. Run initial full load';
PRINT '4. Deploy modified ADF pipelines';
GO
```

---

**Document Version**: 1.0
**Last Updated**: 2025-12-10
**Author**: Claude (Anthropic AI)
**For**: Azure Synapse to Azure SQL Incremental Load Implementation
