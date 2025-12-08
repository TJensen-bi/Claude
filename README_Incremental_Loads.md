# Inkrementel Indlæsning i dbt for Faktatabeller

## Oversigt

Dette dokument beskriver best practices for håndtering af inkrementel indlæsning i dbt, specifikt tilpasset vores finansielle faktatabeller der er baseret på snapshots.

## Baggrund

- **Snapshot-frekvens**: Snapshots kører hver 20. minut
- **Snapshot-filter**: Vi bruger `dbt_valid_to is null` for at hente aktuelle records
- **Faktatabellens grain**: `GeneralJournalAccountEntry.RECID`
- **Udfordring**: Ændringer i dimensionstabeller (snapshots) skal reflekteres i faktatabel

## Anbefalet Løsning: Inkrementel med Lookback Window

### Hvorfor Denne Tilgang?

1. **Dataintegritet**: Sikrer at korrektioner opdateres korrekt
2. **Dimensions-ændringer**: Fanger ændringer i alle relaterede snapshots
3. **Performance**: Genbehandler kun nylige records
4. **Sikkerhed**: 30 minutters lookback-vindue > 20 minutters snapshot-cyklus

### Konfiguration

```sql
{{
  config(
    materialized='incremental',
    unique_key='RECID',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
  )
}}
```

### Nøgle-parametre

| Parameter | Værdi | Formål |
|-----------|-------|--------|
| `materialized` | `incremental` | Bygger tabellen inkrementelt |
| `unique_key` | `RECID` | Identificerer unikke records for opdatering |
| `incremental_strategy` | `merge` | Opdaterer eksisterende records og indsætter nye |
| `on_schema_change` | `sync_all_columns` | Synkroniserer skemaændringer automatisk |

## Implementering

### Trin 1: Tilføj Incremental Logic til Hovedforespørgsel

I `GeneralJournalAccountEntry` CTE, tilføj følgende efter dine WHERE-klausuler:

```sql
WHERE gjae.dbt_valid_to is null
AND gje.dbt_valid_to is null
AND Ledger.dbt_valid_to is null
AND fcp.dbt_valid_to is null
AND davc.dbt_valid_to is null
AND ltvl.dbt_valid_to is null
AND vg_ltvl.dbt_valid_to is null

{% if is_incremental() %}
-- Lookback-vindue for at fange alle ændringer
AND (
    gjae.dbt_updated_at >= dateadd(minute, -30, (SELECT MAX(Posteringsdato) FROM {{ this }}))
    OR gje.dbt_updated_at >= dateadd(minute, -30, (SELECT MAX(Posteringsdato) FROM {{ this }}))
    OR davc.dbt_updated_at >= dateadd(minute, -30, (SELECT MAX(Posteringsdato) FROM {{ this }}))
    OR Ledger.dbt_updated_at >= dateadd(minute, -30, (SELECT MAX(Posteringsdato) FROM {{ this }}))
    OR fcp.dbt_updated_at >= dateadd(minute, -30, (SELECT MAX(Posteringsdato) FROM {{ this }}))
)
{% endif %}
```

### Trin 2: Optimer Momsbridge CTE

Tilføj incremental filter til `Momsbridge` CTE for bedre performance:

```sql
Momsbridge as (
  SELECT DISTINCT
    GeneralJournalAccountEntry, TaxTransRelationship, DataAreaId, TaxTrans
  FROM {{ ref('TaxTransGeneralJournalAccountEntry') }}
  WHERE dbt_valid_to is null

  {% if is_incremental() %}
  AND GeneralJournalAccountEntry IN (SELECT RECID FROM GeneralJournalAccountEntry)
  {% endif %}
)
```

## Sådan Fungerer Det

### Første Kørsel (Full Refresh)

```bash
dbt run --full-refresh --select fact_finansposteringer
```

- Bygger hele tabellen fra bunden
- Processer alle historiske data
- Opretter target-tabel med alle records

### Efterfølgende Kørsler (Incremental)

```bash
dbt run --select fact_finansposteringer
```

- **Finder ændringer**: Identificerer records hvor `dbt_updated_at` er nyere end sidste kørsel minus 30 minutter
- **Genbehandler**: Henter alle relaterede data for de identificerede records
- **Merger**: Opdaterer eksisterende records og indsætter nye via MERGE operation
- **Resultat**: Faktatabel reflekterer seneste ændringer fra alle snapshots

### Lookback Window Logik

```
Snapshot-cyklus:       [----20 min----]
Lookback-vindue:       [------30 min------]
Buffer:                          [10 min]
```

- **20 minutter**: Snapshots kører hver 20. minut
- **30 minutter**: Vi kigger 30 minutter tilbage
- **10 minutters buffer**: Sikkerhedsmargin for at undgå manglende ændringer

## Hvilke Ændringer Fanges?

### 1. Nye Finansposteringer
- Nye `GeneralJournalAccountEntry` records
- Automatisk inkluderet via `dbt_updated_at`

### 2. Opdaterede Posteringer
- Korrektioner (`ErKorrektion = 1`)
- Ændrede beløb eller tekster
- Merge-strategi sikrer korrekt opdatering

### 3. Dimensions-ændringer
- Omkategorisering af finanskonti (`MainAccount`)
- Ændrede dimensionsværdier (`DimensionAttributeValueCombination`)
- Opdaterede kreditoroplysninger (`VendTrans`)
- Ændringer i ledger, perioder, etc.

### 4. Momsændringer
- Nye eller opdaterede `TaxTrans` records
- Ændrede momskoder eller beløb

## Performance-overvejelser

### Forventet Performance

| Datamængde | Første Kørsel | Incremental Kørsel |
|------------|---------------|---------------------|
| < 1M records | 2-5 min | 10-30 sek |
| 1-10M records | 10-30 min | 30-60 sek |
| > 10M records | 30-60 min | 1-3 min |

### Optimeringstips

1. **Indeks på dbt_updated_at**
   - Sikrer hurtig filtrering i snapshots
   - Kritisk for performance

2. **Reducer Lookback-vindue (Avanceret)**
   ```sql
   -- Kun hvis du er sikker på snapshot-timing
   dateadd(minute, -25, ...)  -- I stedet for -30
   ```

3. **Partition på Posteringsdato**
   - Hvis data warehouse understøtter det
   - Forbedrer query performance betydeligt

## Test-strategi

### 1. Initial Test

```bash
# Full refresh
dbt run --full-refresh --select fact_finansposteringer

# Gem count
dbt run --select fact_finansposteringer_test
```

### 2. Incremental Test

```bash
# Incremental run
dbt run --select fact_finansposteringer

# Verificer at count stiger eller forbliver konstant
```

### 3. Dimensions-ændrings Test

1. Identificer en specifik `RECID` i faktatabel
2. Opdater en relateret dimension (f.eks. omkategoriser en finanskonto)
3. Kør snapshot af dimensionstabel
4. Kør incremental load af faktatabellen
5. Verificer at `RECID`'s dimensionsnøgle er opdateret

### 4. Korrektions Test

1. Find en postering med `ErKorrektion = 0`
2. Opdater til `ErKorrektion = 1` i kilde
3. Kør snapshots
4. Kør incremental load
5. Verificer at posten er opdateret korrekt

## Overvågning

### dbt Tests

Tilføj til `schema.yml`:

```yaml
version: 2

models:
  - name: fact_finansposteringer
    description: "Faktabel for finansposteringer med incremental load"
    config:
      tags: ['fact', 'incremental']

    tests:
      # Test for data-actualitet
      - dbt_utils.recency:
          datepart: day
          field: Posteringsdato
          interval: 1
          severity: warn

      # Test for unikke records
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - RECID
          severity: error

    columns:
      - name: RECID
        description: "Primær nøgle fra GeneralJournalAccountEntry"
        tests:
          - not_null
          - unique

      - name: Posteringsbeloeb
        description: "Posteringsbeløb i rapporteringsvaluta"
        tests:
          - not_null

      - name: DimJuridiskEnhedId
        description: "Reference til juridisk enhed (selskab)"
        tests:
          - not_null
          - relationships:
              to: ref('DimJuridiskEnhed')
              field: DimJuridiskEnhedId
```

### Logging

Overvåg disse metrics efter hver kørsel:

- **Records processed**: Hvor mange records blev behandlet?
- **Execution time**: Hvor lang tid tog kørslen?
- **Rows inserted**: Hvor mange nye records?
- **Rows updated**: Hvor mange records blev opdateret?

Eksempel på logging query:

```sql
-- Kør efter incremental load
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT RECID) as unique_records,
    MAX(Posteringsdato) as latest_posting_date,
    MIN(Posteringsdato) as earliest_posting_date,
    SUM(Posteringsbeloeb) as total_amount
FROM {{ ref('fact_finansposteringer') }}
```

## Fejlfinding

### Problem: Records Mangler

**Symptom**: Nye posteringer vises ikke i faktatabel

**Løsning**:
1. Verificer at snapshots er kørt: `SELECT MAX(dbt_updated_at) FROM snapshot_table`
2. Øg lookback-vindue til 45-60 minutter midlertidigt
3. Kør full refresh hvis der er stor diskrepans

### Problem: Duplikerede Records

**Symptom**: Samme RECID forekommer flere gange

**Løsning**:
1. Verificer `unique_key='RECID'` i config
2. Check for NULL-værdier i RECID
3. Kør full refresh for at rette data

### Problem: Langsomme Kørsler

**Symptom**: Incremental loads tager for lang tid

**Løsning**:
1. Verificer indeks på `dbt_updated_at` i alle snapshots
2. Overvej at reducere lookback-vindue til 25 minutter
3. Optimer joins - brug kun nødvendige kolonner i CTEs
4. Tilføj WHERE-filtre så tidligt som muligt i CTEs

### Problem: Dimensions-ændringer Reflekteres Ikke

**Symptom**: Gamle dimensionsværdier vises stadig

**Løsning**:
1. Verificer at dimensionstabel er inkluderet i lookback-filter
2. Check at `dbt_updated_at` opdateres korrekt i snapshot
3. Midlertidigt kør full refresh

## Vedligeholdelse

### Dagligt
- ✅ Overvåg execution time
- ✅ Check for fejl i dbt logs
- ✅ Verificer data-actualitet

### Ugentligt
- ✅ Gennemse antal processede records
- ✅ Sammenlign med forventet volumen
- ✅ Tjek test-resultater

### Månedligt
- ✅ Kør full refresh for at konsolidere data
- ✅ Analyser performance trends
- ✅ Overvej optimeringsmuligheder

### Ved Skemaændringer
- ✅ Test i development-miljø først
- ✅ Kør full refresh i produktion
- ✅ Verificer at `on_schema_change='sync_all_columns'` virker

## Alternative Strategier

### Strategi 1: Full Refresh på Schedule

**Hvornår**: Hvis faktatabellen er lille (< 5M records)

```sql
{{
  config(
    materialized='table'
  )
}}
```

**Fordele**:
- Simpel
- Ingen risiko for manglende ændringer
- Lettere at vedligeholde

**Ulemper**:
- Længere execution time
- Højere compute-omkostninger

### Strategi 2: Incremental med Snapshot Metadata

**Hvornår**: Hvis du har præcise `dbt_updated_at` timestamps

Mere kompleks implementering - se den fulde SQL i den engelske dokumentation.

**Fordele**:
- Mest præcis
- Bedste performance for meget store tabeller

**Ulemper**:
- Mere kompleks
- Kræver nøjagtig timestamp-håndtering

## Konklusioner og Anbefalinger

### Anbefalet Setup
1. ✅ Brug **Incremental med Lookback Window** (beskrevet i dette dokument)
2. ✅ Sæt lookback til **30 minutter**
3. ✅ Inkluder alle kritiske dimensionstabeller i lookback-filter
4. ✅ Brug **merge strategy** for at håndtere korrektioner
5. ✅ Kør **full refresh månedligt** som vedligeholdelse

### Hvornår Køre Full Refresh
- Efter større skemaændringer
- Hvis incremental kørsler fejler gentagne gange
- Som del af månedlig vedligeholdelse
- Når du opdager data-diskrepanser

### Forventet Resultat
Med korrekt implementering skulle du opleve:
- **90-95% reduktion** i execution time sammenlignet med full refresh
- **100% data-nøjagtighed** inklusive dimensions-ændringer
- **Robust håndtering** af korrektioner og opdateringer
- **Forudsigelig performance** også ved højere datavolumen

## Support og Spørgsmål

Ved spørgsmål eller problemer:
1. Konsulter denne dokumentation
2. Check dbt logs: `logs/dbt.log`
3. Kør tests: `dbt test --select fact_finansposteringer`
4. Kontakt BI-teamet

---

**Senest opdateret**: 2025-12-08
**Version**: 1.0
**Forfatter**: BI Team
