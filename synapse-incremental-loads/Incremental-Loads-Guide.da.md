# Omfattende Guide til Inkrementelle Indlæsninger fra Azure Synapse til Azure SQL Database

## Executive Summary

Denne guide giver anbefalinger til implementering af inkrementelle indlæsninger fra Azure Synapse Analytics til Azure SQL Database for Microsoft Dynamics 365 Finance & Operations (F&O) data. Den nuværende implementering bruger full loads (drop og genopret), som kan optimeres ved hjælp af inkrementelle indlæsningsmønstre.

## Indholdsfortegnelse

1. [Nuværende Tilstand Analyse](#nuværende-tilstand-analyse)
2. [Inkrementelle Indlæsningsstrategier](#inkrementelle-indlæsningsstrategier)
3. [Anbefalet Tilgang](#anbefalet-tilgang)
4. [Implementeringsvejledning](#implementeringsvejledning)
5. [Pipeline Modifikationer](#pipeline-modifikationer)
6. [Metadata Administration](#metadata-administration)
7. [Best Practices](#best-practices)
8. [Performance Overvejelser](#performance-overvejelser)

---

## Nuværende tilstand analyse

### Nuværende pipeline 

De eksisterende pipeline udfører **full loads**:

```sql
-- Pre-copy script dropper hele tabellen
DROP TABLE IF EXISTS [schema].[table_name]

-- Genskaber derefter og indlæser al data
-- tableOption: "autoCreate"
```

### Begrænsninger ved full loads

- **Høje Dataoverførselsomkostninger**: Overfører hele tabeller gentagne gange
- **Øget Indlæsningstid**: Behandler alle rækker uanset ændringer
- **Ressourcekrævende**: Højt CPU, hukommelse og I/O forbrug
- **Længere Nedetid**: Tabeller utilgængelige under drop/genopret
- **Netværksbåndbredde**: Overfører uændrede data

---

## Inkrementelle Indlæsningsstrategier

### 1. Watermark-Baseret (High Watermark)

**Bedst til**: Tabeller med pålidelige tidsstempel eller sekventielle ID kolonner

**Hvordan det virker**:
- Track det sidst behandlede tidsstempel/ID (watermark)
- Indlæs kun records hvor tidsstempel/ID > sidste watermark
- Opdater watermark efter succesfuld indlæsning

**Fordele**:
- Simpel at implementere
- Virker med de fleste tabeller
- Lav overhead

**Ulemper**:
- Fanger ikke sletninger
- Kræver tidsstempel eller sekventiel kolonne
- Opdateringer til gamle records kan blive overset

### 2. Change Tracking (SQL Server Native)

**Bedst til**: Azure SQL miljøer med Change Tracking aktiveret

**Hvordan det virker**:
- SQL Server tracker ændringer (INSERT, UPDATE, DELETE)
- Query change tracking tabeller for modificerede records
- Synkroniser ændringer inkrementelt

**Fordele**:
- Fanger alle ændringstyper (INSERT, UPDATE, DELETE)
- Indbygget SQL Server feature
- Minimal overhead

**Ulemper**:
- Kræver Change Tracking aktiveret på kilde
- Retention periode begrænsninger
- Muligvis ikke tilgængelig i Synapse dedicated pool

### 3. Change Data Capture (CDC)

**Bedst til**: Enterprise scenarios der kræver fuld audit trail

**Hvordan det virker**:
- Fanger alle DML operationer fra transaktionslog
- Giver før/efter værdier
- Komplet ændringshistorik

**Fordele**:
- Komplet audit trail
- Fanger alle ændringer
- Historisk analysemulighed

**Ulemper**:
- Højere overhead
- Mere kompleks implementering
- Muligvis ikke tilgængelig i alle Synapse konfigurationer

### 4. Delta Lake Pattern

**Bedst til**: Storskala data lake arkitekturer

**Hvordan det virker**:
- Gem data i Delta Lake format
- Brug MERGE operationer
- Time travel og versionering muligheder

**Fordele**:
- ACID transaktioner
- Schema evolution
- Time travel

**Ulemper**:
- Kræver Delta Lake infrastruktur
- Yderligere kompleksitet

---

## Anbefalet Tilgang

### Primær anbefaling: **Watermark-Baserede inkrementelle indlæsninger**

For dit D365 F&O scenario anbefaler jeg **Watermark (High Watermark) tilgangen** fordi:

1. **D365 F&O Tabeller** har typisk pålidelige audit kolonner:
   - `MODIFIEDDATETIME` - Sidste modificeret tidsstempel
   - `RECID` - Sekventiel record identifier
   - `CREATEDDATETIME` - Oprettelses tidsstempel

2. **Simplere implementering**: Minimale ændringer til eksisterende pipelines

3. **Omkostningseffektiv**: Reducerer dataoverførsel og behandling

4. **Bevist mønster**: Veletableret i Azure Data Factory

### Strategi oversigt

```
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────┐
│ Azure Synapse   │      │  Watermark       │      │  Azure SQL DB   │
│  (Kilde)        │─────>│  Tracking Tabel  │─────>│  (Destination)  │
│  D365 F&O Data  │      │  Sidste Load Tid │      │  Måltabeller    │
└─────────────────┘      └──────────────────┘      └─────────────────┘
         │                        │                         │
         │                        │                         │
         v                        v                         v
   Hent nye/ændrede    Opdater watermark          MERGE/Upsert
   records             efter succes               ændrede data
```

---

## Implementeringsvejledning

### Trin 1: Opret Watermark kontroltabel

Opret en kontroltabel i Azure SQL Database til at tracke det sidst indlæste tidsstempel for hver tabel:

```sql
-- Udfør i Azure SQL Database
CREATE TABLE [control].[TableConfig] (
    TableID INT IDENTITY(1,1) PRIMARY KEY,
    SourceSchema NVARCHAR(128) NOT NULL,
    SourceTable NVARCHAR(255) NOT NULL,
    TargetSchema NVARCHAR(128) NOT NULL,
    TargetTable NVARCHAR(255) NOT NULL,
    WatermarkColumn NVARCHAR(128) NOT NULL,
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

-- Initialiser med F&O tabeller
INSERT INTO [control].[TableConfig]
    (SourceSchema, SourceTable, TargetSchema, TargetTable, WatermarkColumn, PrimaryKeyColumns, WatermarkValue, LoadStatus, IsActive, LoadPriority)
VALUES
    ('dbo', 'generaljournalaccountentry', 'dbo', 'generaljournalaccountentry', 'MODIFIEDDATETIME', 'RECID', '1900-01-01', 'Not Started', 1, 100);
```

### Trin 2: Opret staging tabeller i Azure SQL DB

```sql
-- Opret staging schema
CREATE SCHEMA [staging];

-- Staging tabeller vil blive auto-oprettet eller du kan pre-oprette dem
-- Navngivningskonvention: staging.{TableName}_Stage
```

### Trin 3: Opret merge stored procedure (Upsert Logik)

```sql
CREATE PROCEDURE [control].[usp_MergeIncrementalData]
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

    -- Byg match betingelse for primærnøgler
    SELECT @MatchCondition = STRING_AGG(
        'target.[' + value + '] = source.[' + value + ']',
        ' AND '
    )
    FROM STRING_SPLIT(@PrimaryKeyColumns, ',');

    -- Hent alle kolonner undtagen primærnøgler til UPDATE
    SELECT @UpdateSet = STRING_AGG(
        'target.[' + COLUMN_NAME + '] = source.[' + COLUMN_NAME + ']',
        ', '
    )
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @StagingSchema
        AND TABLE_NAME = @StagingTable
        AND COLUMN_NAME NOT IN (SELECT value FROM STRING_SPLIT(@PrimaryKeyColumns, ','));

    -- Hent alle kolonner til INSERT
    SELECT @InsertColumns = STRING_AGG('[' + COLUMN_NAME + ']', ', '),
           @InsertValues = STRING_AGG('source.[' + COLUMN_NAME + ']', ', ')
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @StagingSchema
        AND TABLE_NAME = @StagingTable
    ORDER BY ORDINAL_POSITION;

    -- Byg MERGE statement
    SET @MergeSql = '
    MERGE [' + @TargetSchema + '].[' + @TargetTable + '] AS target
    USING [' + @StagingSchema + '].[' + @StagingTable + '] AS source
    ON ' + @MatchCondition + '
    WHEN MATCHED THEN
        UPDATE SET ' + @UpdateSet + '
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (' + @InsertColumns + ')
        VALUES (' + @InsertValues + ');';

    -- Udfør merge
    EXEC sp_executesql @MergeSql;

    -- Returner påvirkede rækker
    SELECT @@ROWCOUNT AS RowsAffected;
END;
```

---

## Best Practices

### 1. Initial full load strategi

Før aktivering af inkrementelle indlæsninger:

```sql
-- Trin 1: Udfør initial full load ved hjælp af eksisterende pipeline
-- Trin 2: Sæt initial watermark til nuværende max værdi
UPDATE [control].[TableConfig]
SET WatermarkValue = (
    SELECT MAX(MODIFIEDDATETIME)
    FROM [source_schema].[TableName]
),
LoadStatus = 'Initial Load Complete',
LastLoadDateTime = GETUTCDATE()
WHERE SourceTable = 'TableName';

-- Trin 3: Skift til inkrementel pipeline
```

### 2. Håndter primærnøgle

For D365 F&O tabeller, almindelige primærnøgler:

- **RECID**: Universal unik identifier
- **Sammensatte Nøgler**: Flere kolonner (tjek `INFORMATION_SCHEMA.KEY_COLUMN_USAGE`)

### 3. Overvågning og alarmer

Opret en overvågningsvisning:

```sql
CREATE VIEW [control].[vw_LoadMonitoring] AS
SELECT
    SourceTable,
    LoadStatus,
    LastLoadDateTime,
    RowsLoaded,
    DATEDIFF(HOUR, LastLoadDateTime, GETUTCDATE()) AS HoursSinceLastLoad,
    WatermarkValue,
    ErrorMessage
FROM [control].[TableConfig];

-- Query for fejlede indlæsninger
SELECT * FROM [control].[vw_LoadMonitoring]
WHERE LoadStatus = 'Failed'
ORDER BY LastLoadDateTime DESC;

-- Query for forældet data (ikke indlæst i 24 timer)
SELECT * FROM [control].[vw_LoadMonitoring]
WHERE HoursSinceLastLoad > 24
ORDER BY HoursSinceLastLoad DESC;
```

### 4. Fejlhåndtering

Tilføj retry logik og notifikationer i din pipeline configuration.

### 5. Håndtering af sletninger

Inkrementelle indlæsninger med watermarks fanger ikke sletninger. Muligheder:

**Mulighed A**: Periodisk fuld afstemning
```sql
-- Planlæg ugentlig full load for at fange sletninger
```

**Mulighed B**: Soft deletes
```sql
-- Hvis kilde understøtter soft delete kolonne (f.eks. IsDeleted)
WHERE MODIFIEDDATETIME > @Watermark OR IsDeleted = 1
```

---

## Performance Overvejelser

### 1. Indexeringsstrategi

**Kilde (Synapse)**:
```sql
-- Opret indexes på watermark kolonner
CREATE INDEX IX_TableName_ModifiedDateTime
ON [schema].[TableName] (MODIFIEDDATETIME)
INCLUDE (RECID);
```

**Destination (Azure SQL DB)**:
```sql
-- Sikr primærnøgle indexes eksisterer
-- Tilføj covering indexes til almindelige queries
CREATE INDEX IX_TableName_ModifiedDateTime
ON [schema].[TableName] (MODIFIEDDATETIME);
```

### 2. Staging Tabel Optimering

```sql
-- Brug clustered columnstore for store staging tabeller
CREATE TABLE [staging].[LargeTable_Stage] (
    -- kolonner
) WITH (CLUSTERED COLUMNSTORE INDEX);

-- Brug heap for små staging tabeller
CREATE TABLE [staging].[SmallTable_Stage] (
    -- kolonner
) WITH (HEAP);
```

### 3. Parallel Behandling

Modificer pipeline til at behandle flere tabeller parallelt ved hjælp af ForEach aktivitet med batch størrelse konfiguration.

---

## Sammenfatning og Næste Skridt

### Fordele

1. **Performance**: 10-100x hurtigere indlæsninger for tabeller med <10% daglige ændringer
2. **Omkostning**: 90-99% reduktion i dataoverførselsomkostninger
3. **Friskhed**: Hyppigere indlæsningsvinduer muligt
4. **Ressourceforbrug**: Lavere compute og storage footprint

### Anbefalede næste skridt

1. **Start Småt**: Implementer for generaljournalaccountentry tabel først
2. **Mål**: Sammenlign før/efter metrics
3. **Udvid**: Rul gradvist ud til flere tabeller
4. **Overvåg**: Etabler dashboards og alarmer
5. **Optimer**: Finjuster baseret på faktiske mønstre

### Succeskriterier

Track disse KPI'er:
- Pipeline eksekveringstid (før vs efter)
- Dataoverførselsvolumen (GB)
- Pipeline succesrate (%)
- Datafriskhed (lag tid)
- Omkostning per pipeline kørsel

