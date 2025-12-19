# Microsoft Fabric Pipeline Logging & Metrics Solution

A comprehensive logging and validation framework for Microsoft Fabric Data Pipelines, providing end-to-end observability for data ingestion workflows.

## Overview

This solution implements automated logging, metrics collection, and data quality validation for Fabric pipelines using Delta Lake tables, PySpark notebooks, and native Fabric capabilities.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Source Database                              │
│                   (AdventureWorks SQL)                          │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Fabric Data Pipeline                            │
│  ┌──────────────┐     ┌──────────────┐     ┌─────────────────┐ │
│  │ LookupTables │────▶│ ForEachTable │────▶│ ValidateBronze  │ │
│  └──────────────┘     │  - CopyTable │     │     Layer       │ │
│                       │  - LogMetrics│     └─────────────────┘ │
│                       └──────────────┘                          │
└─────────────────────────────────────────────────────────────────┘
                      │                           │
                      ▼                           ▼
         ┌───────────────────────┐   ┌──────────────────────────┐
         │  Bronze Lakehouse     │   │  ControlLakehouse        │
         │  ├── Files/           │   │  ├── Tables/             │
         │  │   └── adventureworks│  │  │   ├── CopyActivityLog│
         │  │       └── YYYYMMDD/ │   │  │   ├── BronzeValidation│
         │  │           └── *.parquet│ │  │   └── ChecksumValidation│
         └───────────────────────┘   └──────────────────────────┘
```

## Features

✅ **Automated Metrics Logging**
- Capture copy activity metrics (rows, data volume, duration, throughput)
- Track success/failure status for each table
- Handle concurrent writes with retry logic

✅ **Data Quality Validation**
- Row count validation (source vs. Bronze)
- Financial data checksum validation
- Cross-layer validation (Bronze → Silver → Gold)

✅ **Production-Ready**
- Delta Lake storage for ACID transactions
- Concurrent write handling
- Error tracking and logging
- Centralized monitoring

✅ **Pure Fabric Ecosystem**
- No external dependencies
- Uses Fabric Lakehouses, Notebooks, and Pipelines
- SQL analytics endpoint for reporting

## Components

### 1. Delta Tables (ControlLakehouse)

| Table | Purpose | Key Metrics |
|-------|---------|-------------|
| `CopyActivityLog` | Copy activity metrics | Rows copied, data size, throughput, duration |
| `BronzeValidationLog` | Row count validation | Source vs. Bronze row counts, match status |
| `ChecksumValidation` | Financial accuracy | Column sum validation, tolerance checks |
| `MedallionLayerValidation` | Cross-layer validation | Bronze → Silver → Gold consistency |

### 2. Notebooks

| Notebook | Purpose | Trigger |
|----------|---------|---------|
| `Initialize_Control_Tables` | Create Delta tables | Run once (setup) |
| `Log_Copy_Metrics` | Log pipeline metrics | After each copy activity |
| `Bronze_Validation` | Validate Bronze layer | After all copies complete |
| `Checksum_Validation` | Validate financial data | Optional/scheduled |
| `Monitoring_Dashboard` | View metrics | On-demand |

### 3. Pipeline

- **Adventureworks_Fabric_Logging**: Main orchestration pipeline with integrated logging

## Quick Start

### Prerequisites

- Microsoft Fabric workspace with Lakehouse and Pipeline capabilities
- Source database (AdventureWorks or similar)
- Fabric capacity or trial

### Setup (10 minutes)

1. **Create Lakehouses**
   ```
   - Bronze (for raw data)
   - ControlLakehouse (for metrics)
   ```

2. **Import Notebooks**
   - Upload all notebooks from `/notebooks` folder
   - Attach ControlLakehouse to each notebook

3. **Initialize Control Tables**
   - Open `Initialize_Control_Tables` notebook
   - Run all cells
   - Verify 4 Delta tables created

4. **Configure Notebooks**
   - Update JDBC connection details in validation notebooks
   - Set ControlLakehouse absolute path in `Bronze_Validation`

5. **Import Pipeline**
   - Create new pipeline: `Adventureworks_Fabric_Logging`
   - Switch to JSON view
   - Paste content from `/pipelines/Adventureworks_Fabric_Logging.json`
   - Update connection IDs and workspace/artifact IDs

6. **Run Pipeline**
   - Execute pipeline
   - Monitor notebook activities
   - Check ControlLakehouse for logs

## Configuration

### Update JDBC Connection

In `Bronze_Validation` and `Checksum_Validation` notebooks:

```python
jdbc_url = "jdbc:sqlserver://YOUR-SERVER.database.windows.net:1433;database=AdventureWorks"
jdbc_properties = {
    "user": "YOUR-USERNAME",
    "password": "YOUR-PASSWORD",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
```

### Update ControlLakehouse Path

In `Bronze_Validation` and `Log_Copy_Metrics` notebooks:

```python
# Get IDs from ControlLakehouse URL
WORKSPACE_ID = "your-workspace-id"
CONTROL_LAKEHOUSE_ID = "your-control-lakehouse-id"

control_path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{CONTROL_LAKEHOUSE_ID}/Tables/BronzeValidationLog"
```

### Update Pipeline Connection IDs

In pipeline JSON, update:
- `connection`: Your source database connection ID
- `workspaceId`: Your Fabric workspace ID
- `artifactId`: Your Bronze lakehouse ID

## Usage

### View Metrics

**Option 1: SQL Queries**
```sql
-- Recent pipeline runs
SELECT
    TableName,
    RowsCopied,
    DataWrittenMB,
    ThroughputMBps,
    ExecutionStatus,
    CreatedDate
FROM CopyActivityLog
ORDER BY CreatedDate DESC;
```

**Option 2: Monitoring Dashboard**
- Run `Monitoring_Dashboard` notebook
- View aggregated metrics and charts

**Option 3: Power BI**
- Connect to ControlLakehouse SQL endpoint
- Build custom reports

### Monitor Validation

```sql
-- Check validation status
SELECT
    TableName,
    SourceRowCount,
    BronzeRowCount,
    RowCountMatch,
    ValidationStatus
FROM BronzeValidationLog
WHERE ValidationDateTime >= DATEADD(day, -1, GETDATE())
ORDER BY ValidationDateTime DESC;
```

### Check Failed Activities

```sql
-- Find failures
SELECT
    PipelineRunID,
    TableName,
    ErrorMessage,
    CreatedDate
FROM CopyActivityLog
WHERE ExecutionStatus = 'Failed'
ORDER BY CreatedDate DESC;
```

## Key Metrics

Track these KPIs for pipeline health:

- **Throughput**: MB/s data transfer rate
- **Success Rate**: % of successful copy activities
- **Data Freshness**: Time since last successful run
- **Row Count Accuracy**: % of tables passing validation
- **Financial Accuracy**: Checksum validation pass rate

## Troubleshooting

### Issue: Notebooks not found in pipeline
**Solution**: Ensure notebook names match exactly (case-sensitive) and are in the same workspace

### Issue: CopyActivityLog empty
**Solution**: Verify ControlLakehouse is attached to `Log_Copy_Metrics` notebook

### Issue: Validation fails - PATH_NOT_FOUND
**Solution**: Check Bronze lakehouse is attached and execution_date matches folder structure

### Issue: ProtocolChangedException
**Solution**: Retry logic handles this automatically; increase `max_retries` if needed

### Issue: Writing to wrong lakehouse
**Solution**: Use absolute ABFS path instead of relative `Tables/` path

## Performance Considerations

- **Parallel Processing**: Pipeline uses `batchCount: 4` for concurrent table copies
- **Retry Logic**: Handles concurrent Delta Lake writes automatically
- **Lakehouse Separation**: Keeps data and control/logs separated
- **Delta Lake**: ACID transactions ensure data consistency

## Best Practices

1. **Centralized Logging**: Keep all metrics in ControlLakehouse
2. **Absolute Paths**: Use ABFS paths when writing to non-default lakehouses
3. **Error Handling**: Always log both success and failure cases
4. **Validation Timing**: Run validation immediately after pipeline completion
5. **Monitoring**: Set up alerts for failed validations or low throughput

## Future Enhancements

See [FUTURE_FEATURES.md](documentation/FUTURE_FEATURES.md) for planned improvements including:
- Automated alerting
- Historical trend analysis
- Schema drift detection
- Data lineage tracking
- Advanced anomaly detection

## File Structure

```
fabric-logging-metrics/
├── README.md
├── notebooks/
│   ├── Initialize_Control_Tables.py
│   ├── Log_Copy_Metrics.py
│   ├── Bronze_Validation.py
│   ├── Checksum_Validation.py
│   └── Monitoring_Dashboard.py
├── pipelines/
│   └── Adventureworks_Fabric_Logging.json
├── sql-queries/
│   ├── monitoring-queries.sql
│   ├── validation-queries.sql
│   └── performance-queries.sql
└── documentation/
    ├── SETUP_GUIDE.md
    ├── FUTURE_FEATURES.md
    └── ARCHITECTURE.md
```

## Support

For issues or questions:
1. Check troubleshooting section above
2. Review setup guide in `/documentation`
3. Consult Microsoft Fabric documentation

## License

This solution is provided as-is for use with Microsoft Fabric.

## Contributors

Built for AdventureWorks data pipeline monitoring and validation.

---

**Version**: 1.0.0
**Last Updated**: December 2025
**Fabric Compatibility**: Microsoft Fabric (all regions)
