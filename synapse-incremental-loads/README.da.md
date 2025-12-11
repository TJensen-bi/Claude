# Inkrementelle Indlæsninger fra Azure Synapse til Azure SQL Database

## 📋 Oversigt

Dette repository indeholder omfattende dokumentation og eksempler til implementering af inkrementelle indlæsninger fra Azure Synapse Analytics til Azure SQL Database for Microsoft Dynamics 365 Finance & Operations (F&O) data.

## 🎯 Hvilken guide skal du bruge?

### ⭐ **ANBEFALET: Forenklet Single-Pipeline Tilgang** ⭐

**Fil**: [`Simplified-Single-Pipeline-Approach.da.md`](Simplified-Single-Pipeline-Approach.da.md)

**Brug denne hvis:**
- ✅ Du ønsker den simpleste, mest vedligeholdelsesvenlige løsning
- ✅ Du har flere tabeller at synkronisere (eller planlægger at tilføje flere)
- ✅ Du ønsker centraliseret konfiguration
- ✅ Du foretrækker minimal vedligeholdelse

**Arkitektur:**
- 1 kontroltabel (`control.TableConfig`)
- 1 master pipeline (håndterer alle tabeller)
- ForEach loop behandler tabeller dynamisk
- Tilføj nye tabeller med en simpel SQL INSERT

**Opsætningstid**: ~30 minutter

**Pipeline Fil**: [`master-incremental-pipeline.json`](master-incremental-pipeline.json)

---

### 📚 Omfattende dybdegående guide

**Fil**: [`Incremental-Loads-Guide.md`](Incremental-Loads-Guide.md)

**Brug denne hvis:**
- 📖 Du ønsker at forstå alle inkrementelle indlæsningsstrategier
- 📖 Du har brug for detaljerede forklaringer af arkitekturen
- 📖 Du ønsker vejledning i performance optimering
- 📖 Du har brug for fejlfindingsinformation

**Indhold:**
- Sammenligning af alle inkrementelle indlæsningsstrategier
- Detaljeret implementeringsvejledning
- Performance optimeringsteknikker
- Best practices og fejlfinding
- Migreringsplanlægning

---

### ⚡ Hurtigstart Guide

**Fil**: [`Quick-Start-Incremental-Loads.md`](Quick-Start-Incremental-Loads.md)

**Brug denne hvis:**
- ⚡ Du ønsker trin-for-trin opsætningsinstruktioner
- ⚡ Du foretrækker copy-paste SQL scripts
- ⚡ Du har brug for hurtige resultater
- ⚡ Du ønsker at teste med én tabel først

**Indhold:**
- 5-trins opsætningsproces
- Klar-til-brug SQL scripts
- Testprocedurer
- Almindelige D365 F&O tabelkonfigurationer

---

### 🔧 Eksempel: Individuel Tabel Pipeline

**Fil**: [`example-incremental-pipeline.json`](example-incremental-pipeline.json)

**Brug denne hvis:**
- Du kun har 1-2 tabeller at synkronisere
- Du foretrækker separate pipelines per tabel
- Du har specifikke tilpasningsbehov per tabel

**Bemærk**: For de fleste use cases er **master pipeline tilgangen overlegen**.

---

## 🚀 Anbefalet Implementeringsvej

### For De Fleste Brugere (Flere Tabeller)

```
1. Læs: Simplified-Single-Pipeline-Approach.md
   └─ Forstå arkitekturen (5 min)

2. Udfør: SQL Setup Script
   └─ Opret kontroltabel og stored procedures (5 min)

3. Konfigurer: Registrer Dine Tabeller
   └─ INSERT rækker i control.TableConfig (2 min)

4. Deploy: Master Pipeline
   └─ Importer master-incremental-pipeline.json til ADF (10 min)

5. Initial Indlæsning: Kør Eksisterende Full Load Pipelines
   └─ Engangsfuldindlæsning for hver tabel

6. Initialiser: Sæt Watermarks
   └─ UPDATE control.TableConfig med nuværende max værdier (2 min)

7. Test: Kør Master Pipeline
   └─ Verificer at inkrementelle indlæsninger virker

8. Overvåg: Tjek control.TableConfig
   └─ Se indlæsningsstatus og statistikker
```

**Samlet Tid**: ~30 minutter (plus initial full load tid)

---

### For Enkelt Tabel / POC

```
1. Læs: Quick-Start-Incremental-Loads.md
   └─ Følg trin-for-trin vejledning

2. Brug: example-incremental-pipeline.json
   └─ Deploy til din testtabel

3. Test og Valider
   └─ Verificer data nøjagtighed

4. Skaler: Skift til Master Pipeline
   └─ Brug Simplified-Single-Pipeline-Approach.md når klar
```

---

## 📊 Arkitektur Sammenligning

| Feature | Multi-Pipeline | Single Master Pipeline |
|---------|---------------|----------------------|
| **Pipelines at Vedligeholde** | 1 per tabel (100+) | 1 total |
| **Kontroltabeller** | 2 tabeller | 1 tabel |
| **Konfiguration** | Spredt i pipeline JSON | Centraliseret i SQL tabel |
| **Tilføjelse af Nye Tabeller** | Deploy ny pipeline | INSERT én SQL række |
| **Overvågning** | Tjek hver pipeline | Enkelt tabel query |
| **Vedligeholdelsesomfang** | Højt | Lavt |
| **Opsætningskompleksitet** | Høj | Lav |
| **Fleksibilitet** | Per-tabel tilpasning | Prioritetsbaseret behandling |
| **Anbefalet Til** | 1-5 tabeller | 5+ tabeller |

---

## 💡 Nøglefordele

### Performance
- **10-100x hurtigere** eksekveringstider
- Indlæser kun ændrede data (ikke hele tabeller)
- Parallel behandling support

### Omkostningsbesparelser
- **90-99% reduktion** i dataoverførselsomkostninger
- Lavere compute ressourceforbrug
- Reduceret storage I/O

### Operationel
- Hyppigere opdateringsintervaller muligt
- Centraliseret overvågning og kontrol
- Forenklet vedligeholdelse og fejlfinding

---

## 📁 Filoversigt

| Fil | Formål | Størrelse | Hvornår at Bruge |
|------|---------|------|------------|
| **Simplified-Single-Pipeline-Approach.md** | Komplet guide til master pipeline | Fuld | Primær implementering |
| **master-incremental-pipeline.json** | Master pipeline template | ADF JSON | Primær implementering |
| **Incremental-Loads-Guide.md** | Omfattende dybdegående | Detaljeret | Reference/læring |
| **Quick-Start-Incremental-Loads.md** | Hurtig opsætningsguide | Koncis | Hurtigstart/POC |
| **example-incremental-pipeline.json** | Enkelt-tabel pipeline | ADF JSON | Kun enkelt tabel |
| **README-Incremental-Loads.md** (denne fil) | Navigationsvejledning | Oversigt | Start her |

---

## 🔑 Nøglebegreber

### Watermark Pattern
- Tracker sidst behandlede tidsstempel/ID per tabel
- Indlæser kun records med tidsstempel > sidste watermark
- Effektiv og simpel at implementere

### Kontroltabel
- Centralt register over alle tabeller der skal synkroniseres
- Gemmer konfiguration (schema, primærnøgler, watermark kolonne)
- Tracker indlæsningsstatus og historik

### Staging + Merge (UPSERT)
- Kopier ændrede records til staging tabel
- MERGE til target (INSERT nye, UPDATE eksisterende)
- Sikrer data konsistens

### ForEach Loop
- Enkelt pipeline behandler flere tabeller
- Parallel eksekvering (konfigurerbar batch størrelse)
- Dynamisk konfiguration fra kontroltabel

---

## 📖 Almindelige D365 F&O Tabeller

De fleste D365 F&O tabeller inkluderer disse audit kolonner:

| Kolonne | Type | Formål |
|--------|------|---------|
| `MODIFIEDDATETIME` | DATETIME2 | Sidste ændringstidsstempel ⭐ |
| `CREATEDDATETIME` | DATETIME2 | Oprettelsestidsstempel |
| `RECID` | BIGINT | Unikt record ID (ofte PK) |
| `MODIFIEDBY` | NVARCHAR | Bruger der ændrede |

**Anbefalet Watermark Kolonne**: `MODIFIEDDATETIME` ⭐

**Almindelig Primærnøgle**: `RECID` eller forretningsnøgle (f.eks. `ACCOUNTNUM`)

---

## 🔍 Hurtig SQL Reference

### Tjek Tabelstruktur
```sql
-- Verificer at watermark kolonne eksisterer
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'YourTable'
  AND COLUMN_NAME IN ('MODIFIEDDATETIME', 'CREATEDDATETIME', 'RECID');

-- Find primærnøgle
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE TABLE_NAME = 'YourTable'
  AND OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1;
```

### Overvåg Indlæsninger (Single Pipeline Tilgang)
```sql
-- Vis alle tabelstatusser
SELECT SourceTable, LoadStatus, LastLoadDateTime, RowsLoaded
FROM [control].[TableConfig]
ORDER BY LoadPriority, SourceTable;

-- Tjek for fejl
SELECT SourceTable, ErrorMessage, LastLoadDateTime
FROM [control].[TableConfig]
WHERE LoadStatus = 'Failed';

-- Sammendragsstatistikker
SELECT
    COUNT(*) AS TotalTables,
    SUM(CASE WHEN LoadStatus = 'Success' THEN 1 ELSE 0 END) AS Successful,
    SUM(CASE WHEN LoadStatus = 'Failed' THEN 1 ELSE 0 END) AS Failed,
    SUM(RowsLoaded) AS TotalRowsLoaded
FROM [control].[TableConfig]
WHERE IsActive = 1;
```

---

## ⚙️ Konfigurationseksempler

### Registrer en Tabel
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

### Deaktiver en Tabel Midlertidigt
```sql
UPDATE [control].[TableConfig]
SET IsActive = 0
WHERE SourceTable = 'AssetBook';
```

### Sæt Høj Prioritet
```sql
UPDATE [control].[TableConfig]
SET LoadPriority = 10  -- Lavere = højere prioritet
WHERE SourceTable = 'CriticalTable';
```

---

## 🆘 Fejlfinding

### Pipeline Samler Ikke Tabeller Op
```sql
-- Tjek aktive tabeller
SELECT * FROM [control].[TableConfig] WHERE IsActive = 1;
```

### Ingen Rækker Kopieret
```sql
-- Tjek om watermark er foran data
SELECT
    SourceTable,
    WatermarkValue,
    (SELECT MAX(MODIFIEDDATETIME) FROM [dbo].[AssetBook]) AS CurrentMax
FROM [control].[TableConfig]
WHERE SourceTable = 'AssetBook';

-- Nulstil watermark hvis nødvendigt
UPDATE [control].[TableConfig]
SET WatermarkValue = '2025-01-01'
WHERE SourceTable = 'AssetBook';
```

### Merge Fejl
```sql
-- Verificer at primærnøgle er korrekt
SELECT * FROM [control].[TableConfig] WHERE SourceTable = 'AssetBook';

-- Tjek for duplikater i staging
SELECT RECID, COUNT(*)
FROM [staging].[AssetBook_Stage]
GROUP BY RECID
HAVING COUNT(*) > 1;
```

---

## 📞 Support

Ved problemer eller spørgsmål:

1. **Tjek fejlfindingssektionerne** i den relevante guide
2. **Gennemgå fejlbeskeder** i `control.TableConfig.ErrorMessage`
3. **Verificer konfiguration** i kontroltabellen
4. **Tjek ADF pipeline kørselshistorik** for detaljerede fejl

---

## 🎓 Læringssti

### Begynder
1. Start med **Quick-Start-Incremental-Loads.md**
2. Test med én tabel
3. Gennemgå resultater og forstå mønsteret

### Mellem
1. Læs **Simplified-Single-Pipeline-Approach.md**
2. Opsæt master pipeline for 5-10 tabeller
3. Overvåg og optimer

### Avanceret
1. Gennemgå **Incremental-Loads-Guide.md** for dyb forståelse
2. Implementer brugerdefinerede optimeringer
3. Overvej avancerede mønstre (partitionering, CDC, etc.)

---

## 📈 Succeskriterier

Track disse KPI'er for at måle succes:

- ⏱️ **Pipeline Eksekveringstid**: Før vs. efter sammenligning
- 💾 **Dataoverførselsvolumen**: GB overført per kørsel
- ✅ **Succesrate**: Procentdel af succesfulde indlæsninger
- 🕐 **Data Friskhed**: Tidsforskydning mellem kilde og mål
- 💰 **Omkostning**: Azure forbrug på dataflytning

**Forventede Forbedringer:**
- Eksekveringstid: 90% reduktion
- Dataoverførsel: 95% reduktion
- Omkostning: 90-99% besparelser
- Friskhed: Hyppigere opdateringer muligt

---

## 🏁 Hurtig Beslutningsmatrix

**Vælg Master Pipeline (Anbefalet) hvis:**
- ✅ Du har 5+ tabeller (eller vil have)
- ✅ Tabeller følger lignende mønstre
- ✅ Du ønsker minimal vedligeholdelse
- ✅ Du foretrækker SQL-baseret konfiguration

**Vælg Individuelle Pipelines hvis:**
- 🔧 Du kun har 1-2 tabeller
- 🔧 Hver tabel kræver unik logik
- 🔧 Du har specifikke tilpasningskrav

**Når i tvivl, start med Master Pipeline** - det er nemmere at vedligeholde og skalere.

---

## 📝 Versionshistorik

- **v1.0** - Initial omfattende guide
- **v1.1** - Tilføjet forenklet single-pipeline tilgang (anbefalet)
- **v1.2** - Denne navigations README

---

## ✅ Hurtig Tjekliste for Implementering

- [ ] Læs Simplified-Single-Pipeline-Approach.md
- [ ] Opret control schema og tabel i Azure SQL DB
- [ ] Opret stored procedures (4 i alt)
- [ ] Registrer dine tabeller i control.TableConfig
- [ ] Deploy master-incremental-pipeline.json til ADF
- [ ] Opdater dataset referencer i pipeline
- [ ] Test forbindelse til Synapse og Azure SQL DB
- [ ] Udfør initial full load for hver tabel
- [ ] Sæt initiale watermark værdier i kontroltabel
- [ ] Kør master pipeline og verificer
- [ ] Tjek control.TableConfig for status
- [ ] Opsæt overvågningsqueries/dashboard
- [ ] Planlæg pipeline trigger

---

**Anbefalet Startpunkt**: [`Simplified-Single-Pipeline-Approach.md`](Simplified-Single-Pipeline-Approach.md)

**Spørgsmål?** Alle guides inkluderer FAQ sektioner og fejlfindingsinformation.
