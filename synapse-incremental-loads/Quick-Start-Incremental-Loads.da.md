# Hurtigstart Guide: Opsætning af Inkrementelle Indlæsninger

Dette er en komprimeret, trin-for-trin guide til at komme i gang med inkrementelle indlæsninger fra Azure Synapse til Azure SQL Database.

## Forudsætninger

- Azure SQL Database (destination)
- Azure Synapse Analytics (kilde med D365 F&O data)
- Azure Data Factory
- Passende tilladelser på begge databaser

## 5-Trins Opsætning

### Trin 1: Opret Kontrol Infrastruktur (5 minutter)

Udfør dette script i **Azure SQL Database**:

```sql
-- Opret schemas
CREATE SCHEMA control;
CREATE SCHEMA staging;
GO

-- Opret watermark tracking tabel
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
GO

-- Registrer din første tabel (AssetBook eksempel)
INSERT INTO [control].[TableConfig]
VALUES (
    'AssetBook',                    -- TableName
    'dbo',                          -- SchemaName
    'dbo',                          -- TargetSchema
    'AssetBook',                    -- TargetTable
    'MODIFIEDDATETIME',             -- WatermarkColumn
    'RECID',                        -- PrimaryKeyColumns
    '1900-01-01',                   -- Initial WatermarkValue
    NULL,                           -- LastLoadDateTime
    'Not Started',                  -- LoadStatus
    NULL,                           -- RowsLoaded
    NULL,                           -- ErrorMessage
    1,                              -- IsActive
    100,                            -- LoadPriority
    GETUTCDATE(),                   -- CreatedDate
    GETUTCDATE()                    -- ModifiedDate
);
GO
```

### Trin 2: Opret Stored Procedures (3 minutter)

```sql
-- Procedure 1: Hent primærnøgler
CREATE PROCEDURE [control].[usp_GetTablePrimaryKeys]
    @SchemaName NVARCHAR(128),
    @TableName NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT PrimaryKeyColumns
    FROM [control].[TableConfig]
    WHERE SourceSchema = @SchemaName AND SourceTable = @TableName;
END;
GO

-- Procedure 2: Merge data (forenklet version)
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

    -- Byg match betingelse
    DECLARE @MatchCondition NVARCHAR(MAX);
    SELECT @MatchCondition = STRING_AGG(
        'target.[' + value + '] = source.[' + value + ']', ' AND '
    ) FROM STRING_SPLIT(@PrimaryKeyColumns, ',');

    -- Byg UPDATE SET klausul (alle kolonner undtagen PK)
    DECLARE @UpdateSet NVARCHAR(MAX);
    SELECT @UpdateSet = STRING_AGG(
        'target.[' + COLUMN_NAME + '] = source.[' + COLUMN_NAME + ']', ', '
    )
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @StagingSchema
        AND TABLE_NAME = @StagingTable
        AND COLUMN_NAME NOT IN (SELECT value FROM STRING_SPLIT(@PrimaryKeyColumns, ','));

    -- Byg INSERT kolonner og værdier
    DECLARE @Columns NVARCHAR(MAX), @Values NVARCHAR(MAX);
    SELECT
        @Columns = STRING_AGG('[' + COLUMN_NAME + ']', ', '),
        @Values = STRING_AGG('source.[' + COLUMN_NAME + ']', ', ')
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @StagingSchema AND TABLE_NAME = @StagingTable
    ORDER BY ORDINAL_POSITION;

    -- Udfør MERGE
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

-- Procedure 3: Opdater watermark
CREATE PROCEDURE [control].[usp_UpdateWatermark]
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

-- Procedure 4: Log fejl
CREATE PROCEDURE [control].[usp_LogLoadError]
    @TableID INT,
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE [control].[TableConfig]
    SET LastLoadDateTime = GETUTCDATE(),
        LoadStatus = 'Failed',
        ErrorMessage = @ErrorMessage,
        ModifiedDate = GETUTCDATE()
    WHERE TableID = @TableID;
END;
GO

-- Procedure 5: Hent tabeller til behandling
CREATE PROCEDURE [control].[usp_GetTablesToProcess]
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

### Trin 3: Udfør Initial Full Load (10-60 minutter afhængig af datastørrelse)

Før skift til inkrementelle indlæsninger har du brug for en baseline:

1. **Kør din eksisterende full load pipeline** for AssetBook
2. **Sæt den initiale watermark** til nuværende max værdi:

```sql
-- Efter full load er færdig, sæt watermark til nuværende max
UPDATE [control].[TableConfig]
SET WatermarkValue = (
    SELECT MAX(MODIFIEDDATETIME)
    FROM [your_schema].[AssetBook]
),
LoadStatus = 'Initial Load Complete',
LastLoadDateTime = GETUTCDATE()
WHERE SourceTable = 'AssetBook';
```

### Trin 4: Deploy Inkrementel Pipeline (15 minutter)

Brug `master-incremental-pipeline.json` filen som skabelon.

**Nøgleændringer der er nødvendige:**

1. Opdater dataset referencer til at matche dit miljø:
   - `synw_dataverse_dataset` → dit Synapse dataset
   - `sqldb_control_dataset` → dit Azure SQL dataset
   - `sqldb_staging_dataset` → dit Azure SQL dataset med staging schema

2. Opdater globale parametre til at matche din konfiguration

3. Opret datasets hvis nødvendigt

4. Test med AssetBook først før udrulning til andre tabeller

### Trin 5: Test Din Opsætning

## Test 1: Verificer Ingen Ændringer

```sql
-- Tjek nuværende watermark
SELECT * FROM [control].[TableConfig] WHERE SourceTable = 'AssetBook';

-- Kør inkrementel pipeline
-- Bør færdiggøre hurtigt med 0 rækker kopieret
```

## Test 2: Verificer Inkrementel Indlæsning

```sql
-- I Synapse (kilde), opdater en record (hvis muligt)
-- Eller vent på naturlige dataændringer fra D365 F&O

-- Kør inkrementel pipeline
-- Bør kun kopiere ændrede records

-- Verificer i Azure SQL Database
SELECT * FROM [control].[TableConfig] WHERE SourceTable = 'AssetBook';
-- RowsLoaded bør vise antal ændrede rækker
```

## Test 3: Tjek Overvågning

```sql
-- Vis indlæsningsstatus
SELECT
    SourceTable,
    LoadStatus,
    LastLoadDateTime,
    RowsLoaded,
    WatermarkValue,
    ErrorMessage
FROM [control].[TableConfig]
ORDER BY LastLoadDateTime DESC;
```

---

## Almindelige D365 F&O Tabeller og Deres Watermark Kolonner

| Tabel Navn | Primærnøgle | Watermark Kolonne |
|-----------|-------------|------------------|
| AssetBook | RECID | MODIFIEDDATETIME |
| CustTable | ACCOUNTNUM | MODIFIEDDATETIME |
| VendTable | ACCOUNTNUM | MODIFIEDDATETIME |
| InventTable | ITEMID | MODIFIEDDATETIME |
| SalesTable | SALESID | MODIFIEDDATETIME |
| PurchTable | PURCHID | MODIFIEDDATETIME |

**Bemærk**: De fleste D365 F&O tabeller har `MODIFIEDDATETIME` kolonne. Verificer altid med:

```sql
-- Tjek om tabel har MODIFIEDDATETIME
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'YourTableName'
    AND COLUMN_NAME IN ('MODIFIEDDATETIME', 'CREATEDDATETIME', 'RECID');
```

---

## Tilføjelse af Flere Tabeller

For at tilføje yderligere tabeller til inkrementel indlæsning:

```sql
-- 1. Registrer tabel i watermark tabel
INSERT INTO [control].[TableConfig]
VALUES (
    'CustTable',              -- Dit tabelnavn
    'dbo',                    -- Schema
    'dbo',                    -- TargetSchema
    'CustTable',              -- TargetTable
    'MODIFIEDDATETIME',       -- Watermark kolonne
    'ACCOUNTNUM',             -- PrimaryKeyColumns
    '1900-01-01',             -- Initial værdi
    NULL, 'Not Started', NULL, NULL, 1, 100, GETUTCDATE(), GETUTCDATE()
);

-- 2. Kør initial full load (brug eksisterende pipeline)

-- 3. Sæt initial watermark
UPDATE [control].[TableConfig]
SET WatermarkValue = (SELECT MAX(MODIFIEDDATETIME) FROM [dbo].[CustTable]),
    LoadStatus = 'Initial Load Complete',
    LastLoadDateTime = GETUTCDATE()
WHERE SourceTable = 'CustTable';

-- 4. Næste master pipeline kørsel vil automatisk inkludere denne tabel
```

---

## Overvågningsqueries

### Daglig Sundhedstjek

```sql
-- Tabeller med nylige fejl
SELECT
    SourceTable,
    LoadStatus,
    LastLoadDateTime,
    RowsLoaded,
    ErrorMessage
FROM [control].[TableConfig]
WHERE LoadStatus = 'Failed'
    OR DATEDIFF(HOUR, LastLoadDateTime, GETUTCDATE()) > 24
ORDER BY LastLoadDateTime DESC;

-- Indlæsningsstatistikker (sidste 24 timer)
SELECT
    COUNT(*) AS TotalTables,
    SUM(CASE WHEN LoadStatus = 'Success' THEN 1 ELSE 0 END) AS SuccessfulLoads,
    SUM(CASE WHEN LoadStatus = 'Failed' THEN 1 ELSE 0 END) AS FailedLoads,
    SUM(RowsLoaded) AS TotalRowsLoaded,
    AVG(RowsLoaded) AS AvgRowsPerTable
FROM [control].[TableConfig]
WHERE LastLoadDateTime >= DATEADD(HOUR, -24, GETUTCDATE());
```

---

## Fejlfinding

### Problem: Ingen rækker kopieret

**Årsag**: Watermark værdi foran kildedata
**Løsning**:
```sql
-- Nulstil watermark til tidligere dato
UPDATE [control].[TableConfig]
SET WatermarkValue = DATEADD(DAY, -7, GETUTCDATE())
WHERE SourceTable = 'YourTable';
```

### Problem: Duplikatnøglefejl

**Årsag**: Forkert primærnøgle definition
**Løsning**:
```sql
-- Verificer primærnøgler
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE TABLE_NAME = 'YourTable'
    AND OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1;

-- Opdater hvis nødvendigt
UPDATE [control].[TableConfig]
SET PrimaryKeyColumns = 'RECID'
WHERE SourceTable = 'YourTable';
```

### Problem: Merge timeout

**Årsag**: Stor batch størrelse
**Løsning**: Implementer partitionering eller reducer batch vindue

---

## Pipeline Planlægningsanbefalinger

| Dataændringsfrekvens | Anbefalet Schema |
|----------------------|------------------|
| Høj (>1000 rækker/time) | Hver 15-30 minutter |
| Medium (100-1000 rækker/time) | Time |
| Lav (<100 rækker/time) | Hver 4-6 timer |
| Meget Lav | Dagligt |

**Tip**: Start med timebaserede indlæsninger og juster baseret på overvågningsdata.

---

## Forventede Performance Forbedringer

Baseret på typiske D365 F&O brugsmønstre:

| Metric | Full Load | Inkrementel Load | Forbedring |
|--------|-----------|------------------|-------------|
| Eksekveringstid | 30-60 min | 2-5 min | **~90% hurtigere** |
| Dataoverførsel | 1-10 GB | 10-100 MB | **~99% mindre** |
| DTU/vCore Forbrug | Højt | Lavt | **~80% reduktion** |
| Omkostning per Load | $$$ | $ | **~95% billigere** |

*Resultater varierer baseret på tabelstørrelse og ændringshastighed*

---

## Næste Skridt Efter Opsætning

1. **Aktiver for flere tabeller** (5-10 ad gangen)
2. **Opsæt overvågningsalarmer** (Azure Monitor/Data Factory)
3. **Opret dashboard** (Power BI eller Azure Dashboard)
4. **Dokument tabelspecifikke indstillinger** (watermark kolonner, PKs)
5. **Etabler SLA'er** (dataf Riskheds krav)
6. **Planlæg for sletninger** (periodisk fuld afstemning eller soft deletes)

---

## Support og Yderligere Ressourcer

- **Fuld Guide**: Se `Incremental-Loads-Guide.md` for omfattende dokumentation
- **Eksempel Pipeline**: Se `master-incremental-pipeline.json` for komplet ADF pipeline
- **Microsoft Docs**: [Azure Data Factory Incremental Copy](https://docs.microsoft.com/azure/data-factory/tutorial-incremental-copy-overview)

---

## Tjekliste

- [ ] Oprettet control og staging schemas
- [ ] Oprettet TableConfig tabel
- [ ] Oprettet 5 stored procedures
- [ ] Registreret AssetBook i kontroltabel
- [ ] Udført initial full load
- [ ] Sat initial watermark værdi
- [ ] Oprettet/modificeret ADF datasets for staging
- [ ] Deployed inkrementel pipeline
- [ ] Testet med ingen ændringer (0 rækker)
- [ ] Testet med ændringer (>0 rækker)
- [ ] Verificeret data nøjagtighed i destination
- [ ] Opsat overvågningsqueries
- [ ] Planlagt pipeline

---

**Estimeret Samlet Opsætningstid**: 1-2 timer (ekskluderende initial full load)

**Sværhedsgrad**: Mellem

**Forudsætningsviden**:
- Basis SQL
- Azure Data Factory
- Azure SQL Database administration
