{{
    config(
        materialized='table',
        schema='fact',
        tags=['finance', 'journal_entries', 'd365'],
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_fact_journal_bogfoeringsdato ON {{ this }} (Bogfoeringsdato)",
            "CREATE INDEX IF NOT EXISTS idx_fact_journal_selskab ON {{ this }} (Selskab)",
            "CREATE INDEX IF NOT EXISTS idx_fact_journal_recid ON {{ this }} (RECID)"
        ]
    )
}}

/*
════════════════════════════════════════════════════════════════════════════════
Fact Table: Journal Entries (Finansposteringer)
════════════════════════════════════════════════════════════════════════════════
Purpose: Central fact table for all general ledger journal entries from D365
Author: Forca Analytics Team
Last Modified: {{ run_started_at.strftime('%Y-%m-%d') }}
════════════════════════════════════════════════════════════════════════════════
*/

WITH VendTransLatest AS (
    /*
    Henter den seneste kreditortransaktion pr. bilag for at undgå dubletter.
    Der kan være flere bilagsrækker til et bilag, så vi vælger den seneste række
    baseret på SinkModifiedOn.
    */
    SELECT
        voucher,
        DataAreaId,
        ROW_NUMBER() OVER (
            PARTITION BY voucher, DataAreaId
            ORDER BY SinkModifiedOn DESC
        ) AS rk_nr
    FROM {{ ref('VendTrans') }}
    WHERE dbt_valid_to IS NULL
)

,

GeneralJournalAccountEntry AS (
    SELECT
        gjae.RECID
        ,gjae.MainAccount
        ,gjae.TransactionCurrencyCode AS ValutaKode
        ,gjae.LedgerAccount AS FinansKonto
        ,gjae.ReportingCurrencyAmount AS Posteringsbeloeb
        ,gjae.TransactionCurrencyAmount AS Valutaposteringsbeloeb
        ,gjae.Text AS Posteringstekst
        ,gjae.IsCredit AS ErKredit
        ,CASE WHEN gjae.IsCredit = 1 THEN 'Kredit' ELSE 'Debet' END AS ErKreditTekst
        ,gjae.IsCorrection AS ErKorrektion
        ,CASE WHEN gjae.IsCorrection = 1 THEN 'Ja' ELSE 'Nej' END AS ErKorrektionTekst
        ,gjae.GeneralJournalEntry
        ,gjae.LedgerDimension
        ,gjae.PostingType AS Bogfoeringstype
        ,gjae.SCADeepLink
        ,gjae.SCASourceEntryId
        -- I D365 kan FinTag/RECID være 0 og skal så have default surrogatnøgle (-1)
        ,CASE WHEN gjae.FinTag = 0 THEN -1 ELSE gjae.FinTag END AS FinTag
        ,CAST(gje.createdDateTime AS DATE) AS Posteringsdato
        ,gje.createdBy AS PosteretAf
        ,CAST(gje.AccountingDate AS DATE) AS Bogfoeringsdato
        ,gje.JournalCategory AS Posteringstype
        ,gje.JournalNumber AS Kladdenummer
        ,gje.SubledgerVoucher AS Bilagsnummer
        ,gje.FiscalCalendarPeriod
        ,Ledger.[Name] AS Selskab
        ,fcp.[Type] AS PosteringsPeriodetype
        -- Posteringer på Balancen har typisk ikke en afdeling
        ,COALESCE(davc.AFDELINGVALUE, -1) AS AFDELINGVALUE
        ,COALESCE(davc.COSTCENTERVALUE, -1) AS COSTCENTERVALUE
        ,davc.PROJEKTVALUE
        /*
        Kreditorbilagsnummer: Ikke alle finansposteringer har kreditoroplysninger.
        Vi vælger desuden oprindelige kreditorbilagsnummer, hvis der er flere,
        da Kreditorbilaget skifter nummer, hvis det ikke er blevet godkendt inden et månedsskift.
        */
        ,COALESCE(vt.voucher, vg_ltvl.voucher, -1) AS KreditorBilagsnummer
        ,COALESCE(vt.DataAreaId, vg_ltvl.DataAreaId) AS KreditorSelskab
        ,gje.DocumentDate AS Dokumentdato
    FROM {{ ref('GeneralJournalAccountEntry') }} gjae
    LEFT JOIN {{ ref('GeneralJournalEntry') }} gje
        ON gje.RecId = gjae.GeneralJournalEntry
    LEFT JOIN {{ ref('Ledger') }} Ledger
        ON Ledger.RecId = gje.Ledger
    LEFT JOIN {{ ref('FiscalCalendarPeriod') }} fcp
        ON fcp.RecId = gje.FiscalCalendarPeriod
    LEFT JOIN {{ ref('DimensionAttributeValueCombination') }} AS davc
        ON gjae.LedgerDimension = davc.RECID
    LEFT JOIN VendTransLatest vt
        ON vt.voucher = gje.SubledgerVoucher
        AND vt.DataAreaId = Ledger.Name
        AND vt.rk_nr = 1
    -- Henter original kreditorbilag via voucher group
    LEFT JOIN {{ ref('LedgerTransVoucherLink') }} ltvl
        ON ltvl.Voucher = gje.SubledgerVoucher
        AND ltvl.DataAreaId = Ledger.Name
        AND ltvl.voucher LIKE 'K-%'
    LEFT JOIN {{ ref('LedgerTransVoucherLink') }} vg_ltvl
        ON vg_ltvl.VoucherGroupId = ltvl.VoucherGroupId
        AND vg_ltvl.DataAreaId = ltvl.DataAreaId
        AND vg_ltvl.RecVersion = 1
        AND vg_ltvl.voucher LIKE 'K-%'
    WHERE gjae.dbt_valid_to IS NULL
        AND gje.dbt_valid_to IS NULL
        AND Ledger.dbt_valid_to IS NULL
        AND fcp.dbt_valid_to IS NULL
        AND davc.dbt_valid_to IS NULL
        -- Allow NULL LEFT JOINs - don't filter them out
        AND (ltvl.dbt_valid_to IS NULL OR ltvl.RECID IS NULL)
        AND (vg_ltvl.dbt_valid_to IS NULL OR vg_ltvl.RECID IS NULL)
)

,

Projekt AS (
    SELECT
        ProjektId
        ,DimProjektId
    FROM {{ ref('DimProjekt') }}
)

,

/*
════════════════════════════════════════════════════════════════════════════════
Moms (Tax) CTEs
════════════════════════════════════════════════════════════════════════════════
*/

Momsbridge AS (
    /*
    Bridge tabel til TaxTrans.
    DISTINCT anvendes da der kan være dubletter i kildesystemet.
    Alternativt: Overvej at bruge ROW_NUMBER() for mere deterministisk adfærd.
    */
    SELECT DISTINCT
        GeneralJournalAccountEntry,
        TaxTransRelationship,
        DataAreaId,
        TaxTrans
    FROM {{ ref('TaxTransGeneralJournalAccountEntry') }}
    WHERE dbt_valid_to IS NULL
)

,

Moms AS (
    SELECT
        ttgjae.GeneralJournalAccountEntry
        -- These fields added to GROUP BY for deterministic results
        ,ttgjae.TaxTransRelationship
        ,ttgjae.DataAreaId
        -- Aggregated fields
        ,MAX(tt.JournalNum) AS JournalNum
        ,MAX(tt.Voucher) AS Voucher
        ,CAST(SUM(tt.TaxBaseAmountRep) AS DECIMAL(32,16)) AS TaxBaseAmountRep
        ,MAX(tt.TransDate) AS TransDate
        ,CAST(SUM(tt.TaxValue) AS DECIMAL(32,16)) AS Moms
        ,MAX(tt.TaxCode) AS Momskode
        ,MAX(tt.TaxDirection) AS Momsretning
        ,MAX(tt.TaxGroup) AS Momsgruppe
        ,CAST(SUM(tt.TaxAmountRep) AS DECIMAL(32,16)) AS Momsbeloeb
        ,CAST(SUM(tt.TaxInCostPriceRep) AS DECIMAL(32,16)) AS IFM
        ,MAX(tt.TaxItemGroup) AS Varemomsgruppe
        ,MAX(tt.CurrencyCode) AS MomsValutakode
    FROM Momsbridge AS ttgjae
    INNER JOIN {{ ref('TaxTrans') }} AS tt
        ON tt.RECID = ttgjae.TaxTrans
        AND tt.DataAreaId = ttgjae.DataAreaId
    WHERE tt.dbt_valid_to IS NULL
        AND tt.TaxCode <> 'Ingen moms'
    GROUP BY
        ttgjae.GeneralJournalAccountEntry,
        ttgjae.TaxTransRelationship,
        ttgjae.DataAreaId
)

/*
════════════════════════════════════════════════════════════════════════════════
Final SELECT: Dimension Keys & Measures
════════════════════════════════════════════════════════════════════════════════
*/

SELECT
    -- Dimension Keys
    CASE
        WHEN gjae.FinTag = -1
        THEN {{ generate_Forca_surrogate_key('FinTag, null') }}
        ELSE {{ generate_Forca_surrogate_key('FinTag, Selskab') }}
    END AS DimFinanskoderId
    ,{{ generate_Forca_surrogate_key('MainAccount') }} AS DimFinanskontoId
    ,{{ generate_Forca_surrogate_key('ValutaKode') }} AS DimValutaId
    ,{{ generate_Forca_surrogate_key('Bogfoeringstype') }} AS DimBogfoeringstypeId
    ,{{ generate_Forca_surrogate_key('Posteringstype') }} AS DimFinansPosteringstypeId
    ,{{ generate_Forca_surrogate_key('Posteringsperiodetype') }} AS DimFinansPosteringsPeriodetypeId
    ,{{ generate_Forca_surrogate_key('AFDELINGVALUE') }} AS DimAfdelingId
    ,{{ generate_Forca_surrogate_key('COSTCENTERVALUE') }} AS DimBaererId
    ,{{ generate_Forca_surrogate_key('ProjektId') }} AS DimProjektId
    ,{{ generate_Forca_surrogate_key('Selskab') }} AS DimJuridiskEnhedId
    ,{{ generate_Forca_surrogate_key('Momskode') }} AS DimMomskodeId
    ,{{ generate_Forca_surrogate_key('Momsretning') }} AS DimMomsretningId
    ,{{ generate_Forca_surrogate_key('Momsgruppe') }} AS DimMomsgruppeId
    ,{{ generate_Forca_surrogate_key('Varemomsgruppe') }} AS DimVaremomsgruppeId
    -- Andre selskaber end Forca (1000) ligger ikke i hierarkiet
    ,CASE
        WHEN Selskab != '1000'
        THEN {{ generate_Forca_surrogate_key('-1') }}
        ELSE {{ generate_Forca_surrogate_key('AFDELINGVALUE') }}
    END AS DimForcaAfdelingshierarkiId
    ,{{ generate_Forca_surrogate_key('MainAccount') }} AS DimFinansSumkontoId
    ,{{ generate_Forca_surrogate_key('KreditorBilagsnummer, KreditorSelskab') }} AS DimKreditorId
    ,gjae.Bogfoeringsdato AS DimBogfoeringsDatoId

    -- Degenerate Dimensions & Attributes
    ,gjae.RECID
    ,gjae.FinansKonto
    ,gjae.Posteringstekst
    ,gjae.ErKredit
    ,gjae.ErKreditTekst
    ,gjae.ErKorrektion
    ,gjae.ErKorrektionTekst
    ,gjae.PosteretAf
    ,gjae.Kladdenummer
    ,gjae.Bilagsnummer
    ,gjae.Selskab
    ,gjae.SCADeepLink AS LinkTilBilag
    ,gjae.SCASourceEntryId AS KildeindtastningsId
    ,gjae.Posteringsdato
    ,gjae.Dokumentdato

    -- Measures
    ,gjae.Posteringsbeloeb
    ,gjae.Valutaposteringsbeloeb
    ,moms.Momsbeloeb
    ,moms.IFM
    ,moms.Moms
    ,moms.MomsValutakode

FROM GeneralJournalAccountEntry gjae
LEFT JOIN Moms
    ON moms.GeneralJournalAccountEntry = gjae.RECID
    -- 2 = Tax on base amount (grund beløbet) - Vi vil kun finde moms information på grund beløbet
    AND moms.TaxTransRelationship = 2
LEFT JOIN Projekt pj
    ON pj.ProjektId = gjae.PROJEKTVALUE
