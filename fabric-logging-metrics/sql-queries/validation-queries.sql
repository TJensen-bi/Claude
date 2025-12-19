-- =====================================================
-- Validation Queries for Data Quality Monitoring
-- =====================================================
-- Use these queries to monitor data quality and
-- validation status across Bronze layer
-- =====================================================

-- 1. LATEST VALIDATION STATUS
-- View most recent validation results for all tables
SELECT
    TableName,
    SourceRowCount,
    BronzeRowCount,
    MismatchCount,
    CASE
        WHEN RowCountMatch = 1 THEN '✓ Pass'
        ELSE '✗ Fail'
    END AS Status,
    ValidationDateTime,
    ValidationStatus
FROM BronzeValidationLog
WHERE ValidationDateTime = (
    SELECT MAX(ValidationDateTime)
    FROM BronzeValidationLog
)
ORDER BY TableName;

-- 2. VALIDATION FAILURES REQUIRING ATTENTION
-- List all failed validations in last 7 days
SELECT
    ValidationDateTime,
    PipelineRunID,
    TableName,
    SourceRowCount,
    BronzeRowCount,
    MismatchCount,
    CAST(
        100.0 * MismatchCount / NULLIF(SourceRowCount, 0)
        AS DECIMAL(5,2)
    ) AS MismatchPercent,
    ValidationStatus
FROM BronzeValidationLog
WHERE ValidationStatus IN ('Fail', 'Error')
    AND ValidationDateTime >= DATEADD(DAY, -7, GETDATE())
ORDER BY ValidationDateTime DESC, MismatchCount DESC;

-- 3. VALIDATION SUCCESS RATE TREND
-- Calculate daily validation success rate
SELECT
    CAST(ValidationDateTime AS DATE) AS Date,
    COUNT(*) AS TotalValidations,
    SUM(CASE WHEN ValidationStatus = 'Pass' THEN 1 ELSE 0 END) AS PassedCount,
    SUM(CASE WHEN ValidationStatus = 'Fail' THEN 1 ELSE 0 END) AS FailedCount,
    SUM(CASE WHEN ValidationStatus = 'Error' THEN 1 ELSE 0 END) AS ErrorCount,
    CAST(
        100.0 * SUM(CASE WHEN ValidationStatus = 'Pass' THEN 1 ELSE 0 END) / COUNT(*)
        AS DECIMAL(5,2)
    ) AS SuccessRatePercent
FROM BronzeValidationLog
WHERE ValidationDateTime >= DATEADD(DAY, -30, GETDATE())
GROUP BY CAST(ValidationDateTime AS DATE)
ORDER BY Date DESC;

-- 4. TABLES WITH PERSISTENT VALIDATION ISSUES
-- Identify tables that frequently fail validation
SELECT
    TableName,
    COUNT(*) AS TotalValidations,
    SUM(CASE WHEN ValidationStatus = 'Fail' THEN 1 ELSE 0 END) AS FailureCount,
    CAST(
        100.0 * SUM(CASE WHEN ValidationStatus = 'Fail' THEN 1 ELSE 0 END) / COUNT(*)
        AS DECIMAL(5,2)
    ) AS FailureRatePercent,
    AVG(CAST(MismatchCount AS FLOAT)) AS AvgMismatchCount,
    MAX(ValidationDateTime) AS LastValidationDate
FROM BronzeValidationLog
WHERE ValidationDateTime >= DATEADD(DAY, -30, GETDATE())
GROUP BY TableName
HAVING SUM(CASE WHEN ValidationStatus = 'Fail' THEN 1 ELSE 0 END) > 0
ORDER BY FailureRatePercent DESC, FailureCount DESC;

-- 5. CHECKSUM VALIDATION STATUS
-- View financial data accuracy validation results
SELECT
    TableName,
    ColumnName,
    ROUND(SourceSum, 2) AS SourceSum,
    ROUND(BronzeSum, 2) AS BronzeSum,
    ROUND(Difference, 2) AS Difference,
    CASE
        WHEN ChecksumMatch = 1 THEN '✓ Pass'
        ELSE '✗ Fail'
    END AS Status,
    ValidationDateTime
FROM ChecksumValidation
WHERE ValidationDateTime = (
    SELECT MAX(ValidationDateTime)
    FROM ChecksumValidation
)
ORDER BY TableName, ColumnName;

-- 6. CHECKSUM FAILURES
-- Identify financial data discrepancies
SELECT
    TableName,
    ColumnName,
    ROUND(SourceSum, 2) AS SourceSum,
    ROUND(BronzeSum, 2) AS BronzeSum,
    ROUND(Difference, 2) AS Difference,
    CAST(
        100.0 * Difference / NULLIF(SourceSum, 0)
        AS DECIMAL(10,4)
    ) AS DifferencePercent,
    ValidationDateTime,
    DATEDIFF(HOUR, ValidationDateTime, GETDATE()) AS HoursAgo
FROM ChecksumValidation
WHERE ChecksumMatch = 0
    AND ValidationDateTime >= DATEADD(DAY, -7, GETDATE())
ORDER BY Difference DESC;

-- 7. DATA COMPLETENESS REPORT
-- Analyze row count accuracy by table
SELECT
    TableName,
    AVG(CAST(SourceRowCount AS FLOAT)) AS AvgSourceRows,
    AVG(CAST(BronzeRowCount AS FLOAT)) AS AvgBronzeRows,
    AVG(CAST(MismatchCount AS FLOAT)) AS AvgMismatch,
    CAST(
        100.0 * AVG(CASE WHEN RowCountMatch = 1 THEN 1.0 ELSE 0.0 END)
        AS DECIMAL(5,2)
    ) AS CompletenessPercent,
    COUNT(*) AS ValidationCount
FROM BronzeValidationLog
WHERE ValidationDateTime >= DATEADD(DAY, -30, GETDATE())
GROUP BY TableName
ORDER BY CompletenessPercent, TableName;

-- 8. VALIDATION LAG MONITORING
-- Check how long after copy activity validation runs
SELECT
    v.TableName,
    c.CreatedDate AS CopyCompletedTime,
    v.ValidationDateTime,
    DATEDIFF(MINUTE, c.CreatedDate, v.ValidationDateTime) AS ValidationLagMinutes,
    c.RowsCopied,
    v.SourceRowCount,
    v.ValidationStatus
FROM BronzeValidationLog v
INNER JOIN CopyActivityLog c
    ON v.PipelineRunID = c.PipelineRunID
    AND v.TableName = c.TableName
WHERE v.ValidationDateTime >= DATEADD(DAY, -7, GETDATE())
ORDER BY ValidationLagMinutes DESC;

-- 9. MEDALLION LAYER VALIDATION
-- Cross-layer data consistency check
SELECT
    TableName,
    BronzeRowCount,
    SilverRowCount,
    GoldRowCount,
    CASE
        WHEN BronzeToSilverMatch = 1 THEN '✓'
        ELSE '✗'
    END AS BronzeToSilver,
    CASE
        WHEN SilverToGoldMatch = 1 THEN '✓'
        ELSE '✗'
    END AS SilverToGold,
    ValidationDateTime
FROM MedallionLayerValidation
WHERE ValidationDateTime = (
    SELECT MAX(ValidationDateTime)
    FROM MedallionLayerValidation
)
ORDER BY TableName;

-- 10. DATA QUALITY SCORECARD
-- Comprehensive quality score per table
WITH QualityMetrics AS (
    SELECT
        b.TableName,
        -- Completeness (row count accuracy)
        AVG(CASE WHEN b.RowCountMatch = 1 THEN 100.0 ELSE 0.0 END) AS CompletenessScore,
        -- Accuracy (checksum validation)
        COALESCE(
            AVG(CASE WHEN c.ChecksumMatch = 1 THEN 100.0 ELSE 0.0 END),
            100.0
        ) AS AccuracyScore,
        -- Timeliness (data freshness in hours)
        CASE
            WHEN MAX(b.ValidationDateTime) >= DATEADD(HOUR, -2, GETDATE()) THEN 100.0
            WHEN MAX(b.ValidationDateTime) >= DATEADD(HOUR, -6, GETDATE()) THEN 75.0
            WHEN MAX(b.ValidationDateTime) >= DATEADD(HOUR, -12, GETDATE()) THEN 50.0
            WHEN MAX(b.ValidationDateTime) >= DATEADD(HOUR, -24, GETDATE()) THEN 25.0
            ELSE 0.0
        END AS TimelinessScore
    FROM BronzeValidationLog b
    LEFT JOIN ChecksumValidation c
        ON b.TableName = c.TableName
        AND b.PipelineRunID = c.PipelineRunID
    WHERE b.ValidationDateTime >= DATEADD(DAY, -7, GETDATE())
    GROUP BY b.TableName
)
SELECT
    TableName,
    ROUND(CompletenessScore, 2) AS CompletenessScore,
    ROUND(AccuracyScore, 2) AS AccuracyScore,
    ROUND(TimelinessScore, 2) AS TimelinessScore,
    ROUND(
        (CompletenessScore * 0.4) +
        (AccuracyScore * 0.4) +
        (TimelinessScore * 0.2),
        2
    ) AS OverallQualityScore,
    CASE
        WHEN (CompletenessScore * 0.4 + AccuracyScore * 0.4 + TimelinessScore * 0.2) >= 90 THEN '🟢 Excellent'
        WHEN (CompletenessScore * 0.4 + AccuracyScore * 0.4 + TimelinessScore * 0.2) >= 75 THEN '🟡 Good'
        WHEN (CompletenessScore * 0.4 + AccuracyScore * 0.4 + TimelinessScore * 0.2) >= 50 THEN '🟠 Fair'
        ELSE '🔴 Poor'
    END AS QualityGrade
FROM QualityMetrics
ORDER BY OverallQualityScore DESC;

-- 11. VALIDATION COVERAGE CHECK
-- Ensure all copied tables are being validated
SELECT
    c.TableName,
    MAX(c.CreatedDate) AS LastCopyDate,
    MAX(v.ValidationDateTime) AS LastValidationDate,
    CASE
        WHEN MAX(v.ValidationDateTime) IS NULL THEN '✗ Never Validated'
        WHEN MAX(v.ValidationDateTime) < MAX(c.CreatedDate) THEN '⚠ Validation Outdated'
        ELSE '✓ Up to Date'
    END AS ValidationCoverage
FROM CopyActivityLog c
LEFT JOIN BronzeValidationLog v
    ON c.TableName = v.TableName
WHERE c.CreatedDate >= DATEADD(DAY, -7, GETDATE())
    AND c.ExecutionStatus = 'Success'
GROUP BY c.TableName
ORDER BY ValidationCoverage, LastCopyDate DESC;
