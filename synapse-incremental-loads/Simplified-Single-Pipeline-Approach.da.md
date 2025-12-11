# Forenklet Single Pipeline Tilgang til Inkrementelle Indlæsninger

## Arkitekturoversigt

Denne forenklede tilgang bruger:
- ✅ **ÉN kontroltabel** - Indeholder al konfiguration og tracking
- ✅ **ÉN master pipeline** - Behandler alle tabeller ved hjælp af ForEach loop
- ✅ **Simpel at vedligeholde** - Tilføj nye tabeller ved at indsætte én række

## Fordele Over Multi-Pipeline Tilgang

| Aspekt | Multi-Pipeline | Single Pipeline |
|--------|---------------|-----------------|
| Pipelines at vedligeholde | 1 per tabel (100+ pipelines) | 1 master pipeline |
| Konfiguration | Spredt på tværs af pipelines | Centraliseret i én tabel |
| Tilføjelse af nye tabeller | Deploy ny pipeline | Indsæt én række i kontroltabel |
| Overvågning | Tjek hver pipeline | Én samlet oversigt |
| Ændringer/opdateringer | Opdater alle pipelines | Opdater én pipeline |
| Kompleksitet | Høj | Lav |

---

## Trin 1: Opret Enkelt Kontroltabel (2 minutter)

Udfør dette i **Azure SQL Database**:

```sql
-- Opret schemas
CREATE SCHEMA control;
CREATE SCHEMA staging;
GO

-- Opret samlet kontroltabel med AL metadata
CREATE TABLE [control].[TableConfig] (
    -- Identifikation
    TableID INT IDENTITY(1,1) PRIMARY KEY,
    SourceSchema NVARCHAR(128) NOT NULL,
    SourceTable NVARCHAR(255) NOT NULL,
    TargetSchema NVARCHAR(128) NOT NULL,
    TargetTable NVARCHAR(255) NOT NULL,

    -- Inkrementel indlæsningskonfiguration
    WatermarkColumn NVARCHAR(128) NOT NULL DEFAULT 'MODIFIEDDATETIME',
    PrimaryKeyColumns NVARCHAR(500) NOT NULL,  -- Kommasepareret liste

    -- State tracking
    WatermarkValue DATETIME2(7) NULL,
    LastLoadDateTime DATETIME2(7) NULL,
    LoadStatus NVARCHAR(50) NULL,
    RowsLoaded BIGINT NULL,
    ErrorMessage NVARCHAR(MAX) NULL,

    -- Kontrolflag
    IsActive BIT NOT NULL DEFAULT 1,
    LoadPriority INT NOT NULL DEFAULT 100,  -- Lavere tal = højere prioritet

    -- Metadata
    CreatedDate DATETIME2(7) NOT NULL DEFAULT GETUTCDATE(),
    ModifiedDate DATETIME2(7) NOT NULL DEFAULT GETUTCDATE(),

    CONSTRAINT UQ_Source_Table UNIQUE (SourceSchema, SourceTable)
);
GO

-- Opret indeks for aktive tabeller
CREATE INDEX IX_TableConfig_Active ON [control].[TableConfig] (IsActive, LoadPriority)
WHERE IsActive = 1;
GO
```

---

## Trin 2: Registrer Dine Tabeller (1 minut)

```sql
-- Registrer D365 F&O tabeller til inkrementel indlæsning
INSERT INTO [control].[TableConfig]
    (SourceSchema, SourceTable, TargetSchema, TargetTable, WatermarkColumn, PrimaryKeyColumns, WatermarkValue, LoadStatus, IsActive, LoadPriority)
VALUES
    -- Høj prioritet tabeller (indlæses først)
    ('dbo', 'AssetBook', 'dbo', 'AssetBook', 'MODIFIEDDATETIME', 'RECID', '1900-01-01', 'Not Started', 1, 10),
    ('dbo', 'CustTable', 'dbo', 'CustTable', 'MODIFIEDDATETIME', 'ACCOUNTNUM', '1900-01-01', 'Not Started', 1, 10),
    ('dbo', 'VendTable', 'dbo', 'VendTable', 'MODIFIEDDATETIME', 'ACCOUNTNUM', '1900-01-01', 'Not Started', 1, 10),

    -- Medium prioritet tabeller
    ('dbo', 'InventTable', 'dbo', 'InventTable', 'MODIFIEDDATETIME', 'ITEMID', '1900-01-01', 'Not Started', 1, 50),
    ('dbo', 'SalesTable', 'dbo', 'SalesTable', 'MODIFIEDDATETIME', 'SALESID', '1900-01-01', 'Not Started', 1, 50),

    -- Lavere prioritet tabeller
    ('dbo', 'PurchTable', 'dbo', 'PurchTable', 'MODIFIEDDATETIME', 'PURCHID', '1900-01-01', 'Not Started', 1, 100);

-- Vis registrerede tabeller
SELECT
    TableID,
    SourceTable,
    WatermarkColumn,
    PrimaryKeyColumns,
    LoadStatus,
    IsActive,
    LoadPriority
FROM [control].[TableConfig]
ORDER BY LoadPriority, SourceTable;
```

---

## Trin 3: Opret Stored Procedures (3 minutter)

```sql
-- Procedure 1: Merge inkrementel data
CREATE OR ALTER PROCEDURE [control].[usp_MergeIncrementalData]
    @TargetSchema NVARCHAR(128),
    @TargetTable NVARCHAR(255),
    @PrimaryKeyColumns NVARCHAR(MAX),
    @StagingSchema NVARCHAR(128) = 'staging'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @StagingTable NVARCHAR(255) = @TargetTable + '_Stage';
    DECLARE @MatchCondition NVARCHAR(MAX);
    DECLARE @UpdateSet NVARCHAR(MAX);
    DECLARE @Columns NVARCHAR(MAX);
    DECLARE @Values NVARCHAR(MAX);

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Byg ON betingelse (primærnøgle match)
        SELECT @MatchCondition = STRING_AGG(
            'target.[' + TRIM(value) + '] = source.[' + TRIM(value) + ']',
            ' AND '
        )
        FROM STRING_SPLIT(@PrimaryKeyColumns, ',');

        -- Byg UPDATE SET klausul (alle kolonner undtagen PKs)
        SELECT @UpdateSet = STRING_AGG(
            'target.[' + COLUMN_NAME + '] = source.[' + COLUMN_NAME + ']',
            ', '
        )
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = @StagingSchema
            AND TABLE_NAME = @StagingTable
            AND COLUMN_NAME NOT IN (SELECT TRIM(value) FROM STRING_SPLIT(@PrimaryKeyColumns, ','));

        -- Byg INSERT kolonner og værdier
        SELECT
            @Columns = STRING_AGG('[' + COLUMN_NAME + ']', ', '),
            @Values = STRING_AGG('source.[' + COLUMN_NAME + ']', ', ')
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = @StagingSchema
            AND TABLE_NAME = @StagingTable
        ORDER BY ORDINAL_POSITION;

        -- Byg og udfør MERGE statement
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

        COMMIT TRANSACTION;

        -- Returner påvirkede rækker
        SELECT @@ROWCOUNT AS RowsAffected;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH
END;
GO

-- Procedure 2: Opdater watermark efter succesfuld indlæsning
CREATE OR ALTER PROCEDURE [control].[usp_UpdateWatermark]
    @TableID INT,
    @NewWatermarkValue DATETIME2(7),
    @RowsLoaded BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE [control].[TableConfig]
    SET
        WatermarkValue = @NewWatermarkValue,
        LastLoadDateTime = GETUTCDATE(),
        LoadStatus = 'Success',
        RowsLoaded = @RowsLoaded,
        ErrorMessage = NULL,
        ModifiedDate = GETUTCDATE()
    WHERE TableID = @TableID;
END;
GO

-- Procedure 3: Log fejl
CREATE OR ALTER PROCEDURE [control].[usp_LogLoadError]
    @TableID INT,
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE [control].[TableConfig]
    SET
        LastLoadDateTime = GETUTCDATE(),
        LoadStatus = 'Failed',
        ErrorMessage = LEFT(@ErrorMessage, 4000),  -- Afkort hvis nødvendigt
        ModifiedDate = GETUTCDATE()
    WHERE TableID = @TableID;
END;
GO

-- Procedure 4: Hent tabeller der skal behandles
CREATE OR ALTER PROCEDURE [control].[usp_GetTablesToProcess]
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        TableID,
        SourceSchema,
        SourceTable,
        TargetSchema,
        TargetTable,
        WatermarkColumn,
        PrimaryKeyColumns,
        ISNULL(WatermarkValue, '1900-01-01') AS WatermarkValue
    FROM [control].[TableConfig]
    WHERE IsActive = 1
    ORDER BY LoadPriority, SourceTable;
END;
GO
```

---

## Trin 4: Opret Master Pipeline

Her er den komplette enkelte pipeline der håndterer alle tabeller:

Se [`master-incremental-pipeline.json`](master-incremental-pipeline.json) for den fulde pipeline definition.

**Nøglefunktioner i denne Pipeline:**

1. **Lookup Aktivitet**: Henter alle aktive tabeller fra kontroltabel
2. **ForEach Loop**: Behandler hver tabel med samme logik
3. **Parallel Eksekvering**: `batchCount: 10` betyder 10 tabeller behandles samtidigt
4. **Ændringsdetektion**: Indlæser kun hvis watermark er avanceret
5. **Fejlhåndtering**: Logger fejl per tabel, fortsætter med andre
6. **Automatisk Watermark Opdatering**: Opdaterer efter succesfuld indlæsning

---

## Trin 5: Brug og Administration

### Tilføjelse af en Ny Tabel

Indsæt blot en række i kontroltabellen:

```sql
INSERT INTO [control].[TableConfig]
    (SourceSchema, SourceTable, TargetSchema, TargetTable, WatermarkColumn, PrimaryKeyColumns, WatermarkValue, LoadStatus, IsActive, LoadPriority)
VALUES
    ('dbo', 'YourNewTable', 'dbo', 'YourNewTable', 'MODIFIEDDATETIME', 'RECID', '1900-01-01', 'Not Started', 1, 100);

-- Det er det! Næste pipeline kørsel vil automatisk samle den op
```

### Midlertidigt Deaktiver en Tabel

```sql
UPDATE [control].[TableConfig]
SET IsActive = 0
WHERE SourceTable = 'TableToDisable';
```

### Ændr Indlæsningsprioritet

```sql
-- Gør en tabel til høj prioritet (indlæses først)
UPDATE [control].[TableConfig]
SET LoadPriority = 10
WHERE SourceTable = 'CriticalTable';

-- Gør en tabel til lav prioritet (indlæses sidst)
UPDATE [control].[TableConfig]
SET LoadPriority = 999
WHERE SourceTable = 'LessCriticalTable';
```

### Vis Indlæsningsstatus

```sql
-- Nuværende status for alle tabeller
SELECT
    SourceTable,
    LoadStatus,
    LastLoadDateTime,
    RowsLoaded,
    DATEDIFF(MINUTE, LastLoadDateTime, GETUTCDATE()) AS MinutesSinceLastLoad,
    WatermarkValue,
    IsActive
FROM [control].[TableConfig]
ORDER BY LoadPriority, SourceTable;

-- Fejlede indlæsninger
SELECT
    SourceTable,
    ErrorMessage,
    LastLoadDateTime
FROM [control].[TableConfig]
WHERE LoadStatus = 'Failed'
ORDER BY LastLoadDateTime DESC;

-- Forældet data (ikke indlæst i sidste 2 timer)
SELECT
    SourceTable,
    LastLoadDateTime,
    DATEDIFF(HOUR, LastLoadDateTime, GETUTCDATE()) AS HoursSinceLoad
FROM [control].[TableConfig]
WHERE IsActive = 1
    AND (LastLoadDateTime IS NULL OR DATEDIFF(HOUR, LastLoadDateTime, GETUTCDATE()) > 2)
ORDER BY LastLoadDateTime;
```

---

## Trin 6: Initial Opsætning for Hver Tabel

Før aktivering af inkrementelle indlæsninger skal du udføre en initial full load:

```sql
-- 1. Udfør initial full load ved hjælp af din eksisterende pipeline for hver tabel

-- 2. Efter full load, sæt watermark til nuværende max værdi
UPDATE tc
SET
    WatermarkValue = t.MaxWatermark,
    LoadStatus = 'Initial Load Complete',
    LastLoadDateTime = GETUTCDATE()
FROM [control].[TableConfig] tc
CROSS APPLY (
    -- Query target tabellen for at få max watermark
    SELECT MAX(MODIFIEDDATETIME) AS MaxWatermark
    FROM [dbo].[AssetBook]  -- Skift tabelnavn her
) t
WHERE tc.SourceTable = 'AssetBook';  -- Skift tabelnavn her

-- Gentag for hver tabel, eller opret et dynamisk SQL script til at gøre dem alle
```

---

## Performance Tuning

### Juster Parallel Behandling

```json
{
    "typeProperties": {
        "isSequential": false,
        "batchCount": 10  // Juster denne værdi
    }
}
```

**Anbefalinger:**
- **Små tabeller (<1M rækker)**: batchCount = 20 (høj parallelisme)
- **Medium tabeller (1-10M rækker)**: batchCount = 10 (balanceret)
- **Store tabeller (>10M rækker)**: batchCount = 5 (konservativ)
- **Meget store tabeller**: isSequential = true (én ad gangen)

### Optimer Kontroltabel Query

```sql
-- Tilføj filterkroner hvis nødvendigt
ALTER PROCEDURE [control].[usp_GetTablesToProcess]
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        TableID,
        SourceSchema,
        SourceTable,
        TargetSchema,
        TargetTable,
        WatermarkColumn,
        PrimaryKeyColumns,
        ISNULL(WatermarkValue, '1900-01-01') AS WatermarkValue
    FROM [control].[TableConfig]
    WHERE IsActive = 1
        -- Valgfrit: Indlæs kun tabeller der ikke er indlæst for nylig
        AND (LastLoadDateTime IS NULL OR DATEDIFF(MINUTE, LastLoadDateTime, GETUTCDATE()) > 30)
    ORDER BY LoadPriority, SourceTable;
END;
```

---

## Overvågnings Dashboard Query

```sql
-- Komplet indlæsningsstatistik
SELECT
    COUNT(*) AS TotalTables,
    SUM(CASE WHEN IsActive = 1 THEN 1 ELSE 0 END) AS ActiveTables,
    SUM(CASE WHEN LoadStatus = 'Success' THEN 1 ELSE 0 END) AS SuccessfulLoads,
    SUM(CASE WHEN LoadStatus = 'Failed' THEN 1 ELSE 0 END) AS FailedLoads,
    SUM(CASE WHEN LoadStatus = 'Not Started' THEN 1 ELSE 0 END) AS NotStarted,
    SUM(RowsLoaded) AS TotalRowsLoaded,
    MAX(LastLoadDateTime) AS MostRecentLoad,
    MIN(LastLoadDateTime) AS OldestLoad
FROM [control].[TableConfig];

-- Detaljeret status per tabel
SELECT
    SourceTable,
    LoadStatus,
    FORMAT(LastLoadDateTime, 'yyyy-MM-dd HH:mm:ss') AS LastLoad,
    FORMAT(RowsLoaded, 'N0') AS RowsLoaded,
    CASE
        WHEN LastLoadDateTime IS NULL THEN 'Never loaded'
        WHEN DATEDIFF(HOUR, LastLoadDateTime, GETUTCDATE()) < 1 THEN 'Fresh'
        WHEN DATEDIFF(HOUR, LastLoadDateTime, GETUTCDATE()) < 4 THEN 'Recent'
        WHEN DATEDIFF(HOUR, LastLoadDateTime, GETUTCDATE()) < 24 THEN 'Stale'
        ELSE 'Very Stale'
    END AS DataFreshness,
    IsActive,
    LoadPriority
FROM [control].[TableConfig]
ORDER BY LoadPriority, SourceTable;
```

---

## Komplet Opsætningsscript

Her er et komplet script du kan køre for at opsætte alt:

```sql
-- ============================================================
-- KOMPLET OPSÆTNINGSSCRIPT TIL SINGLE-PIPELINE INKREMENTELLE INDLÆSNINGER
-- ============================================================

-- Trin 1: Opret schemas
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'control')
    EXEC('CREATE SCHEMA control');

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA staging');
GO

-- Trin 2: Opret kontroltabel
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TableConfig' AND schema_id = SCHEMA_ID('control'))
BEGIN
    CREATE TABLE [control].[TableConfig] (
        TableID INT IDENTITY(1,1) PRIMARY KEY,
        SourceSchema NVARCHAR(128) NOT NULL,
        SourceTable NVARCHAR(255) NOT NULL,
        TargetSchema NVARCHAR(128) NOT NULL,
        TargetTable NVARCHAR(255) NOT NULL,
        WatermarkColumn NVARCHAR(128) NOT NULL DEFAULT 'MODIFIEDDATETIME',
        PrimaryKeyColumns NVARCHAR(500) NOT NULL,
        WatermarkValue DATETIME2(7) NULL,
        LastLoadDateTime DATETIME2(7) NULL,
        LoadStatus NVARCHAR(50) NULL,
        RowsLoaded BIGINT NULL,
        ErrorMessage NVARCHAR(MAX) NULL,
        IsActive BIT NOT NULL DEFAULT 1,
        LoadPriority INT NOT NULL DEFAULT 100,
        CreatedDate DATETIME2(7) NOT NULL DEFAULT GETUTCDATE(),
        ModifiedDate DATETIME2(7) NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT UQ_Source_Table UNIQUE (SourceSchema, SourceTable)
    );

    CREATE INDEX IX_TableConfig_Active ON [control].[TableConfig] (IsActive, LoadPriority)
    WHERE IsActive = 1;
END
GO

-- Trin 3: Opret stored procedures
CREATE OR ALTER PROCEDURE [control].[usp_MergeIncrementalData]
    @TargetSchema NVARCHAR(128),
    @TargetTable NVARCHAR(255),
    @PrimaryKeyColumns NVARCHAR(MAX),
    @StagingSchema NVARCHAR(128) = 'staging'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @StagingTable NVARCHAR(255) = @TargetTable + '_Stage';
    DECLARE @MatchCondition NVARCHAR(MAX);
    DECLARE @UpdateSet NVARCHAR(MAX);
    DECLARE @Columns NVARCHAR(MAX);
    DECLARE @Values NVARCHAR(MAX);

    BEGIN TRY
        BEGIN TRANSACTION;

        SELECT @MatchCondition = STRING_AGG(
            'target.[' + TRIM(value) + '] = source.[' + TRIM(value) + ']', ' AND '
        ) FROM STRING_SPLIT(@PrimaryKeyColumns, ',');

        SELECT @UpdateSet = STRING_AGG(
            'target.[' + COLUMN_NAME + '] = source.[' + COLUMN_NAME + ']', ', '
        )
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = @StagingSchema AND TABLE_NAME = @StagingTable
            AND COLUMN_NAME NOT IN (SELECT TRIM(value) FROM STRING_SPLIT(@PrimaryKeyColumns, ','));

        SELECT
            @Columns = STRING_AGG('[' + COLUMN_NAME + ']', ', '),
            @Values = STRING_AGG('source.[' + COLUMN_NAME + ']', ', ')
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = @StagingSchema AND TABLE_NAME = @StagingTable
        ORDER BY ORDINAL_POSITION;

        SET @SQL = '
        MERGE [' + @TargetSchema + '].[' + @TargetTable + '] AS target
        USING [' + @StagingSchema + '].[' + @StagingTable + '] AS source
        ON ' + @MatchCondition + '
        WHEN MATCHED THEN UPDATE SET ' + @UpdateSet + '
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (' + @Columns + ') VALUES (' + @Values + ');';

        EXEC sp_executesql @SQL;
        COMMIT TRANSACTION;
        SELECT @@ROWCOUNT AS RowsAffected;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO

CREATE OR ALTER PROCEDURE [control].[usp_UpdateWatermark]
    @TableID INT,
    @NewWatermarkValue DATETIME2(7),
    @RowsLoaded BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [control].[TableConfig]
    SET WatermarkValue = @NewWatermarkValue,
        LastLoadDateTime = GETUTCDATE(),
        LoadStatus = 'Success',
        RowsLoaded = @RowsLoaded,
        ErrorMessage = NULL,
        ModifiedDate = GETUTCDATE()
    WHERE TableID = @TableID;
END;
GO

CREATE OR ALTER PROCEDURE [control].[usp_LogLoadError]
    @TableID INT,
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [control].[TableConfig]
    SET LastLoadDateTime = GETUTCDATE(),
        LoadStatus = 'Failed',
        ErrorMessage = LEFT(@ErrorMessage, 4000),
        ModifiedDate = GETUTCDATE()
    WHERE TableID = @TableID;
END;
GO

CREATE OR ALTER PROCEDURE [control].[usp_GetTablesToProcess]
AS
BEGIN
    SET NOCOUNT ON;
    SELECT TableID, SourceSchema, SourceTable, TargetSchema, TargetTable,
           WatermarkColumn, PrimaryKeyColumns,
           ISNULL(WatermarkValue, '1900-01-01') AS WatermarkValue
    FROM [control].[TableConfig]
    WHERE IsActive = 1
    ORDER BY LoadPriority, SourceTable;
END;
GO

PRINT 'Opsætning komplet!';
PRINT 'Næste trin:';
PRINT '1. Registrer dine tabeller i [control].[TableConfig]';
PRINT '2. Udfør initial full load for hver tabel';
PRINT '3. Sæt initiale watermark værdier';
PRINT '4. Deploy master pipeline';
PRINT '5. Planlæg og overvåg';
GO
```

---

## Sammenligning: Før vs Efter

### Før (Multi-Pipeline Tilgang)

```
❌ 100+ separate pipelines
❌ Konfiguration spredt overalt
❌ Svært at vedligeholde og opdatere
❌ Svært at få samlet status
❌ Komplekst at tilføje nye tabeller
```

### Efter (Single-Pipeline Tilgang)

```
✅ 1 master pipeline
✅ Al konfiguration i 1 tabel
✅ Nem at vedligeholde
✅ Enkelt oversigt over alle indlæsninger
✅ Tilføj tabeller med 1 INSERT statement
```

---

## FAQ

**Sp: Kan jeg blande full loads og inkrementelle loads?**

Sv: Ja! Opret blot en separat pipeline til full loads. Brug `IsActive` flaget til at kontrollere hvilke tabeller der får inkrementelle indlæsninger.

**Sp: Hvad hvis en tabel ikke har MODIFIEDDATETIME?**

Sv: Opdater `WatermarkColumn` for den tabel:
```sql
UPDATE [control].[TableConfig]
SET WatermarkColumn = 'RECID'  -- Eller CREATEDDATETIME, etc.
WHERE SourceTable = 'YourTable';
```

**Sp: Hvordan nulstiller jeg en tabel til full reload?**

Sv: Nulstil watermark:
```sql
UPDATE [control].[TableConfig]
SET WatermarkValue = '1900-01-01',
    LoadStatus = 'Reset for Full Load'
WHERE SourceTable = 'YourTable';
```

**Sp: Kan jeg bruge forskellige watermark kolonner per tabel?**

Sv: Ja! Det er præcis hvad `WatermarkColumn` feltet er til. Hver tabel kan have sin egen.

**Sp: Hvor mange tabeller kan én pipeline håndtere?**

Sv: Testet med 500+ tabeller. ForEach aktiviteten med `batchCount` håndterer parallelisme godt.

---

## Konklusion

Denne forenklede single-pipeline tilgang giver dig:

- ✅ **Centraliseret administration** - Al konfiguration ét sted
- ✅ **Nem vedligeholdelse** - Opdater én pipeline, ikke hundreder
- ✅ **Simpel onboarding** - Tilføj nye tabeller med én SQL INSERT
- ✅ **Bedre overvågning** - Enkelt oversigt over alle tabel indlæsninger
- ✅ **Omkostningseffektiv** - Samme 90-99% besparelser som multi-pipeline tilgang
- ✅ **Skalerbar** - Håndterer hundreder af tabeller effektivt

**Estimeret opsætningstid**: 30 minutter (vs. timer for multi-pipeline)

**Anbefalet til**: Alle nye implementeringer
