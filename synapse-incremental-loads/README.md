# Incremental Loads from Azure Synapse to Azure SQL Database

## 📋 Overview

This repository contains comprehensive documentation and examples for implementing incremental loads from Azure Synapse Analytics to Azure SQL Database for Microsoft Dynamics 365 Finance & Operations (F&O) data.

## 🎯 Which Guide Should You Use?

### ⭐ **RECOMMENDED: Simplified Single-Pipeline Approach** ⭐

**File**: [`Simplified-Single-Pipeline-Approach.md`](Simplified-Single-Pipeline-Approach.md)

**Use this if:**
- ✅ You want the simplest, most maintainable solution
- ✅ You have multiple tables to sync (or plan to add more)
- ✅ You want centralized configuration
- ✅ You prefer minimal maintenance overhead

**Architecture:**
- 1 control table (`control.TableConfig`)
- 1 master pipeline (handles all tables)
- ForEach loop processes tables dynamically
- Add new tables with a simple SQL INSERT

**Setup Time**: ~30 minutes

**Pipeline File**: [`master-incremental-pipeline.json`](master-incremental-pipeline.json)

---

### 📚 Comprehensive Deep-Dive Guide

**File**: [`Incremental-Loads-Guide.md`](Incremental-Loads-Guide.md)

**Use this if:**
- 📖 You want to understand all incremental load strategies
- 📖 You need detailed explanations of the architecture
- 📖 You want performance optimization guidance
- 📖 You need troubleshooting information

**Contents:**
- Comparison of all incremental load strategies
- Detailed implementation guide
- Performance optimization techniques
- Best practices and troubleshooting
- Migration planning

---

### ⚡ Quick Start Guide

**File**: [`Quick-Start-Incremental-Loads.md`](Quick-Start-Incremental-Loads.md)

**Use this if:**
- ⚡ You want step-by-step setup instructions
- ⚡ You prefer copy-paste SQL scripts
- ⚡ You need quick wins
- ⚡ You want to test with one table first

**Contents:**
- 5-step setup process
- Ready-to-use SQL scripts
- Testing procedures
- Common D365 F&O table configurations

---

### 🔧 Example: Individual Table Pipeline

**File**: [`example-incremental-pipeline.json`](example-incremental-pipeline.json)

**Use this if:**
- You only have 1-2 tables to sync
- You prefer separate pipelines per table
- You have specific customization needs per table

**Note**: For most use cases, the **master pipeline approach is superior**.

---

## 🚀 Recommended Implementation Path

### For Most Users (Multiple Tables)

```
1. Read: Simplified-Single-Pipeline-Approach.md
   └─ Understand the architecture (5 min)

2. Execute: SQL Setup Script
   └─ Create control table and stored procedures (5 min)

3. Configure: Register Your Tables
   └─ INSERT rows into control.TableConfig (2 min)

4. Deploy: Master Pipeline
   └─ Import master-incremental-pipeline.json into ADF (10 min)

5. Initial Load: Run Existing Full Load Pipelines
   └─ One-time full load for each table

6. Initialize: Set Watermarks
   └─ UPDATE control.TableConfig with current max values (2 min)

7. Test: Run Master Pipeline
   └─ Verify incremental loads work

8. Monitor: Check control.TableConfig
   └─ View load status and statistics
```

**Total Time**: ~30 minutes (plus initial full load time)

---

### For Single Table / POC

```
1. Read: Quick-Start-Incremental-Loads.md
   └─ Follow step-by-step guide

2. Use: example-incremental-pipeline.json
   └─ Deploy for your test table

3. Test and Validate
   └─ Verify data accuracy

4. Scale: Switch to Master Pipeline
   └─ Use Simplified-Single-Pipeline-Approach.md when ready
```

---

## 📊 Architecture Comparison

| Feature | Multi-Pipeline | Single Master Pipeline |
|---------|---------------|----------------------|
| **Pipelines to Maintain** | 1 per table (100+) | 1 total |
| **Control Tables** | 2 tables | 1 table |
| **Configuration** | Scattered in pipeline JSON | Centralized in SQL table |
| **Adding New Tables** | Deploy new pipeline | INSERT one SQL row |
| **Monitoring** | Check each pipeline | Single table query |
| **Maintenance Effort** | High | Low |
| **Setup Complexity** | High | Low |
| **Flexibility** | Per-table customization | Priority-based processing |
| **Recommended For** | 1-5 tables | 5+ tables |

---

## 💡 Key Benefits

### Performance
- **10-100x faster** execution times
- Only loads changed data (not entire tables)
- Parallel processing support

### Cost Savings
- **90-99% reduction** in data transfer costs
- Lower compute resource usage
- Reduced storage I/O

### Operational
- More frequent refresh windows possible
- Centralized monitoring and control
- Simplified maintenance and troubleshooting

---

## 📁 File Reference

| File | Purpose | Size | When to Use |
|------|---------|------|------------|
| **Simplified-Single-Pipeline-Approach.md** | Complete guide for master pipeline | Full | Primary implementation |
| **master-incremental-pipeline.json** | Master pipeline template | ADF JSON | Primary implementation |
| **Incremental-Loads-Guide.md** | Comprehensive deep-dive | Detailed | Reference/learning |
| **Quick-Start-Incremental-Loads.md** | Fast setup guide | Concise | Quick start/POC |
| **example-incremental-pipeline.json** | Single-table pipeline | ADF JSON | Single table only |
| **README-Incremental-Loads.md** (this file) | Navigation guide | Overview | Start here |

---

## 🔑 Key Concepts

### Watermark Pattern
- Tracks last processed timestamp/ID per table
- Only loads records with timestamp > last watermark
- Efficient and simple to implement

### Control Table
- Central registry of all tables to sync
- Stores configuration (schema, primary keys, watermark column)
- Tracks load status and history

### Staging + Merge (UPSERT)
- Copy changed records to staging table
- MERGE into target (INSERT new, UPDATE existing)
- Ensures data consistency

### ForEach Loop
- Single pipeline processes multiple tables
- Parallel execution (configurable batch size)
- Dynamic configuration from control table

---

## 📖 Common D365 F&O Tables

Most D365 F&O tables include these audit columns:

| Column | Type | Purpose |
|--------|------|---------|
| `MODIFIEDDATETIME` | DATETIME2 | Last modification timestamp ⭐ |
| `CREATEDDATETIME` | DATETIME2 | Creation timestamp |
| `RECID` | BIGINT | Unique record ID (often PK) |
| `MODIFIEDBY` | NVARCHAR | User who modified |

**Recommended Watermark Column**: `MODIFIEDDATETIME` ⭐

**Common Primary Key**: `RECID` or business key (e.g., `ACCOUNTNUM`)

---

## 🔍 Quick SQL Reference

### Check Table Structure
```sql
-- Verify watermark column exists
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'YourTable'
  AND COLUMN_NAME IN ('MODIFIEDDATETIME', 'CREATEDDATETIME', 'RECID');

-- Find primary key
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE TABLE_NAME = 'YourTable'
  AND OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1;
```

### Monitor Loads (Single Pipeline Approach)
```sql
-- View all table statuses
SELECT SourceTable, LoadStatus, LastLoadDateTime, RowsLoaded
FROM [control].[TableConfig]
ORDER BY LoadPriority, SourceTable;

-- Check for failures
SELECT SourceTable, ErrorMessage, LastLoadDateTime
FROM [control].[TableConfig]
WHERE LoadStatus = 'Failed';

-- Summary statistics
SELECT
    COUNT(*) AS TotalTables,
    SUM(CASE WHEN LoadStatus = 'Success' THEN 1 ELSE 0 END) AS Successful,
    SUM(CASE WHEN LoadStatus = 'Failed' THEN 1 ELSE 0 END) AS Failed,
    SUM(RowsLoaded) AS TotalRowsLoaded
FROM [control].[TableConfig]
WHERE IsActive = 1;
```

---

## ⚙️ Configuration Examples

### Register a Table
```sql
INSERT INTO [control].[TableConfig]
    (SourceSchema, SourceTable, TargetSchema, TargetTable,
     WatermarkColumn, PrimaryKeyColumns, WatermarkValue,
     LoadStatus, IsActive, LoadPriority)
VALUES
    ('dbo', 'AssetBook', 'dbo', 'AssetBook',
     'MODIFIEDDATETIME', 'RECID', '1900-01-01',
     'Not Started', 1, 100);
```

### Disable a Table Temporarily
```sql
UPDATE [control].[TableConfig]
SET IsActive = 0
WHERE SourceTable = 'AssetBook';
```

### Set High Priority
```sql
UPDATE [control].[TableConfig]
SET LoadPriority = 10  -- Lower = higher priority
WHERE SourceTable = 'CriticalTable';
```

---

## 🆘 Troubleshooting

### Pipeline Not Picking Up Tables
```sql
-- Check active tables
SELECT * FROM [control].[TableConfig] WHERE IsActive = 1;
```

### No Rows Copied
```sql
-- Check if watermark is ahead of data
SELECT
    SourceTable,
    WatermarkValue,
    (SELECT MAX(MODIFIEDDATETIME) FROM [dbo].[AssetBook]) AS CurrentMax
FROM [control].[TableConfig]
WHERE SourceTable = 'AssetBook';

-- Reset watermark if needed
UPDATE [control].[TableConfig]
SET WatermarkValue = '2025-01-01'
WHERE SourceTable = 'AssetBook';
```

### Merge Failures
```sql
-- Verify primary key is correct
SELECT * FROM [control].[TableConfig] WHERE SourceTable = 'AssetBook';

-- Check for duplicates in staging
SELECT RECID, COUNT(*)
FROM [staging].[AssetBook_Stage]
GROUP BY RECID
HAVING COUNT(*) > 1;
```

---

## 📞 Support

For issues or questions:

1. **Check the troubleshooting sections** in the relevant guide
2. **Review error messages** in `control.TableConfig.ErrorMessage`
3. **Verify configuration** in control table
4. **Check ADF pipeline run history** for detailed errors

---

## 🎓 Learning Path

### Beginner
1. Start with **Quick-Start-Incremental-Loads.md**
2. Test with one table
3. Review results and understand the pattern

### Intermediate
1. Read **Simplified-Single-Pipeline-Approach.md**
2. Set up master pipeline for 5-10 tables
3. Monitor and optimize

### Advanced
1. Review **Incremental-Loads-Guide.md** for deep understanding
2. Implement custom optimizations
3. Consider advanced patterns (partitioning, CDC, etc.)

---

## 📈 Success Metrics

Track these KPIs to measure success:

- ⏱️ **Pipeline Execution Time**: Before vs. after comparison
- 💾 **Data Transfer Volume**: GB transferred per run
- ✅ **Success Rate**: Percentage of successful loads
- 🕐 **Data Freshness**: Time lag between source and target
- 💰 **Cost**: Azure spend on data movement

**Expected Improvements:**
- Execution time: 90% reduction
- Data transfer: 95% reduction
- Cost: 90-99% savings
- Freshness: More frequent refreshes possible

---

## 🏁 Quick Decision Matrix

**Choose Master Pipeline (Recommended) if:**
- ✅ You have 5+ tables (or will have)
- ✅ Tables follow similar patterns
- ✅ You want minimal maintenance
- ✅ You prefer SQL-based configuration

**Choose Individual Pipelines if:**
- 🔧 You have 1-2 tables only
- 🔧 Each table needs unique logic
- 🔧 You have specific customization requirements

**When in doubt, start with Master Pipeline** - it's easier to maintain and scale.

---

## 📝 Version History

- **v1.0** - Initial comprehensive guide
- **v1.1** - Added simplified single-pipeline approach (recommended)
- **v1.2** - This navigation README

---

## ✅ Quick Checklist for Implementation

- [ ] Read Simplified-Single-Pipeline-Approach.md
- [ ] Create control schema and table in Azure SQL DB
- [ ] Create stored procedures (4 total)
- [ ] Register your tables in control.TableConfig
- [ ] Deploy master-incremental-pipeline.json to ADF
- [ ] Update dataset references in pipeline
- [ ] Test connection to Synapse and Azure SQL DB
- [ ] Perform initial full load for each table
- [ ] Set initial watermark values in control table
- [ ] Run master pipeline and verify
- [ ] Check control.TableConfig for status
- [ ] Set up monitoring queries/dashboard
- [ ] Schedule pipeline trigger

---

**Recommended Starting Point**: [`Simplified-Single-Pipeline-Approach.md`](Simplified-Single-Pipeline-Approach.md)

**Questions?** All guides include FAQ sections and troubleshooting information.
