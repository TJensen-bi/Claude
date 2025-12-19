-- =====================================================
-- Monitoring Queries for Fabric Pipeline Metrics
-- =====================================================
-- Use these queries in ControlLakehouse SQL endpoint
-- to monitor pipeline health and performance
-- =====================================================

-- 1. LATEST PIPELINE RUN SUMMARY
-- View most recent pipeline execution details
SELECT
    PipelineName,
    PipelineRunID,
    COUNT(*) AS TablesProcessed,
    SUM(RowsCopied) AS TotalRowsCopied,
    SUM(DataWrittenMB) AS TotalDataMB,
    AVG(ThroughputMBps) AS AvgThroughput,
    MAX(CreatedDate) AS ExecutionTime,
    SUM(CASE WHEN ExecutionStatus = 'Success' THEN 1 ELSE 0 END) AS SuccessCount,
    SUM(CASE WHEN ExecutionStatus = 'Failed' THEN 1 ELSE 0 END) AS FailedCount
FROM CopyActivityLog
WHERE CreatedDate = (SELECT MAX(CreatedDate) FROM CopyActivityLog)
GROUP BY PipelineName, PipelineRunID;

-- 2. DAILY PIPELINE PERFORMANCE
-- Track performance metrics over last 30 days
SELECT
    CAST(CreatedDate AS DATE) AS ExecutionDate,
    COUNT(DISTINCT PipelineRunID) AS TotalRuns,
    COUNT(DISTINCT TableName) AS TablesProcessed,
    SUM(RowsCopied) AS TotalRowsCopied,
    ROUND(SUM(DataWrittenMB), 2) AS TotalDataWrittenMB,
    ROUND(AVG(CopyDurationSeconds), 2) AS AvgDurationSeconds,
    ROUND(AVG(ThroughputMBps), 2) AS AvgThroughputMBps,
    SUM(CASE WHEN ExecutionStatus = 'Success' THEN 1 ELSE 0 END) AS SuccessfulCopies,
    SUM(CASE WHEN ExecutionStatus = 'Failed' THEN 1 ELSE 0 END) AS FailedCopies
FROM CopyActivityLog
WHERE CreatedDate >= DATEADD(DAY, -30, GETDATE())
GROUP BY CAST(CreatedDate AS DATE)
ORDER BY ExecutionDate DESC;

-- 3. PER-TABLE PERFORMANCE METRICS
-- Analyze performance by table over last 7 days
SELECT
    TableName,
    COUNT(*) AS ExecutionCount,
    AVG(RowsCopied) AS AvgRowsCopied,
    MAX(RowsCopied) AS MaxRowsCopied,
    MIN(RowsCopied) AS MinRowsCopied,
    ROUND(AVG(DataWrittenMB), 2) AS AvgDataWrittenMB,
    ROUND(AVG(CopyDurationSeconds), 2) AS AvgDurationSeconds,
    ROUND(AVG(ThroughputMBps), 2) AS AvgThroughputMBps,
    MAX(CreatedDate) AS LastExecutionDate
FROM CopyActivityLog
WHERE ExecutionStatus = 'Success'
    AND CreatedDate >= DATEADD(DAY, -7, GETDATE())
GROUP BY TableName
ORDER BY AvgRowsCopied DESC;

-- 4. FAILED ACTIVITIES REQUIRING ATTENTION
-- List all failed copy activities with error details
SELECT
    PipelineRunID,
    TableName,
    ErrorMessage,
    CreatedDate,
    DATEDIFF(HOUR, CreatedDate, GETDATE()) AS HoursAgo
FROM CopyActivityLog
WHERE ExecutionStatus = 'Failed'
    AND CreatedDate >= DATEADD(DAY, -7, GETDATE())
ORDER BY CreatedDate DESC;

-- 5. PIPELINE SUCCESS RATE TREND
-- Calculate success rate by day
SELECT
    CAST(CreatedDate AS DATE) AS Date,
    COUNT(*) AS TotalActivities,
    SUM(CASE WHEN ExecutionStatus = 'Success' THEN 1 ELSE 0 END) AS SuccessCount,
    SUM(CASE WHEN ExecutionStatus = 'Failed' THEN 1 ELSE 0 END) AS FailedCount,
    CAST(
        100.0 * SUM(CASE WHEN ExecutionStatus = 'Success' THEN 1 ELSE 0 END) / COUNT(*)
        AS DECIMAL(5,2)
    ) AS SuccessRatePercent
FROM CopyActivityLog
WHERE CreatedDate >= DATEADD(DAY, -30, GETDATE())
GROUP BY CAST(CreatedDate AS DATE)
ORDER BY Date DESC;

-- 6. SLOWEST TABLES
-- Identify tables with longest copy duration
SELECT TOP 10
    TableName,
    AVG(CopyDurationSeconds) AS AvgDurationSeconds,
    AVG(RowsCopied) AS AvgRowsCopied,
    ROUND(AVG(ThroughputMBps), 2) AS AvgThroughputMBps,
    COUNT(*) AS ExecutionCount
FROM CopyActivityLog
WHERE ExecutionStatus = 'Success'
    AND CreatedDate >= DATEADD(DAY, -7, GETDATE())
GROUP BY TableName
ORDER BY AvgDurationSeconds DESC;

-- 7. DATA VOLUME GROWTH
-- Track data growth over time
SELECT
    CAST(CreatedDate AS DATE) AS Date,
    TableName,
    AVG(RowsCopied) AS AvgRows,
    ROUND(AVG(DataWrittenMB), 2) AS AvgDataMB
FROM CopyActivityLog
WHERE ExecutionStatus = 'Success'
    AND CreatedDate >= DATEADD(DAY, -30, GETDATE())
GROUP BY CAST(CreatedDate AS DATE), TableName
ORDER BY Date DESC, TableName;

-- 8. HOURLY EXECUTION PATTERN
-- Analyze when pipelines typically run
SELECT
    DATEPART(HOUR, CreatedDate) AS HourOfDay,
    COUNT(*) AS ExecutionCount,
    AVG(CopyDurationSeconds) AS AvgDurationSeconds,
    ROUND(AVG(ThroughputMBps), 2) AS AvgThroughputMBps
FROM CopyActivityLog
WHERE CreatedDate >= DATEADD(DAY, -7, GETDATE())
GROUP BY DATEPART(HOUR, CreatedDate)
ORDER BY HourOfDay;

-- 9. THROUGHPUT DEGRADATION ALERT
-- Identify tables with declining throughput
WITH RecentThroughput AS (
    SELECT
        TableName,
        AVG(CASE
            WHEN CreatedDate >= DATEADD(DAY, -3, GETDATE())
            THEN ThroughputMBps
        END) AS Recent3DayAvg,
        AVG(CASE
            WHEN CreatedDate >= DATEADD(DAY, -14, GETDATE())
            AND CreatedDate < DATEADD(DAY, -7, GETDATE())
            THEN ThroughputMBps
        END) AS Previous7DayAvg
    FROM CopyActivityLog
    WHERE ExecutionStatus = 'Success'
        AND CreatedDate >= DATEADD(DAY, -14, GETDATE())
    GROUP BY TableName
)
SELECT
    TableName,
    ROUND(Recent3DayAvg, 2) AS Recent3DayAvgMBps,
    ROUND(Previous7DayAvg, 2) AS Previous7DayAvgMBps,
    ROUND(
        100.0 * (Recent3DayAvg - Previous7DayAvg) / NULLIF(Previous7DayAvg, 0),
        2
    ) AS PercentChange
FROM RecentThroughput
WHERE Previous7DayAvg IS NOT NULL
    AND Recent3DayAvg < (Previous7DayAvg * 0.8)  -- Alert if 20% slower
ORDER BY PercentChange;

-- 10. EXECUTION SUMMARY BY PIPELINE RUN
-- Detailed summary for a specific pipeline run
-- Replace @PipelineRunID with actual run ID
DECLARE @PipelineRunID NVARCHAR(255) = 'YOUR-RUN-ID-HERE';

SELECT
    'Pipeline Summary' AS Section,
    PipelineName AS Metric,
    CAST(MIN(CreatedDate) AS VARCHAR(30)) AS Value
FROM CopyActivityLog
WHERE PipelineRunID = @PipelineRunID
GROUP BY PipelineName

UNION ALL

SELECT
    'Execution Metrics',
    'Total Tables',
    CAST(COUNT(*) AS VARCHAR(30))
FROM CopyActivityLog
WHERE PipelineRunID = @PipelineRunID

UNION ALL

SELECT
    'Execution Metrics',
    'Total Rows Copied',
    FORMAT(SUM(RowsCopied), 'N0')
FROM CopyActivityLog
WHERE PipelineRunID = @PipelineRunID

UNION ALL

SELECT
    'Execution Metrics',
    'Total Data Written (MB)',
    CAST(ROUND(SUM(DataWrittenMB), 2) AS VARCHAR(30))
FROM CopyActivityLog
WHERE PipelineRunID = @PipelineRunID

UNION ALL

SELECT
    'Performance',
    'Avg Throughput (MB/s)',
    CAST(ROUND(AVG(ThroughputMBps), 2) AS VARCHAR(30))
FROM CopyActivityLog
WHERE PipelineRunID = @PipelineRunID;
