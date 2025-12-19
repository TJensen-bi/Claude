WITH GeneralJournalAccountEntry as (
    SELECT gjae.RECID
          ,gjae.MainAccount
          ,gjae.TransactionCurrencyCode as ValutaKode
          ,gjae.LedgerAccount as FinansKonto
          ,gjae.ReportingCurrencyAmount as Posteringsbeloeb
          ,gjae.TransactionCurrencyAmount as Valutaposteringsbeloeb
          ,gjae.Text as Posteringstekst
          ,gjae.IsCredit as ErKredit
          ,IIF(gjae.IsCredit=1, 'Kredit', 'Debet') as ErKreditTekst
          ,gjae.IsCorrection as ErKorrektion
          ,IIF(gjae.IsCorrection=1, 'Ja', 'Nej') as ErKorrektionTekst
          ,gjae.GeneralJournalEntry 
          ,gjae.LedgerDimension
          ,gjae.PostingType as Bogfoeringstype
          ,gjae.SCADeepLink
          ,gjae.SCASourceEntryId
          ,IIF(gjae.FinTag = 0, -1, gjae.FinTag) as FinTag /*I D365 kan FinTag/RECID være 0 og skal så have default surrogatnøgle*/
          ,cast(gje.createdDateTime as date) as Posteringsdato
          ,gje.createdBy as PosteretAf 
          ,cast(gje.AccountingDate as date) as Bogfoeringsdato
          ,gje.JournalCategory as Posteringstype
          ,gje.JournalNumber as Kladdenummer
          ,gje.SubledgerVoucher as Bilagsnummer
          ,gje.FiscalCalendarPeriod 
          ,Ledger.[Name] as Selskab
          ,fcp.[Type] as PosteringsPeriodetype
          ,ISNULL(davc.AFDELINGVALUE,-1) as AFDELINGVALUE  /* Posteringer på Balancen har typisk ikke en afdeling */
          ,ISNULL(davc.COSTCENTERVALUE, -1 ) AS COSTCENTERVALUE
          ,davc.PROJEKTVALUE
          ,ISNULL(ISNULL(vt.voucher, vg_ltvl.voucher ),-1) AS KreditorBilagsnummer /*Ikke alle finansposteringer har kreditoroplysninger. Vi vælger desuden oprindelige kreditorbilagsnummer, hvis der er flere, da Kreditorbilaget skifter nummer, hvis det ikke er blevet godkendt inden et månedsskift*/
          ,ISNULL(vt.DataAreaId, vg_ltvl.dataareaid ) AS KreditorSelskab
          ,gje.DocumentDate AS Dokumentdato
    FROM {{ ref('GeneralJournalAccountEntry')  }} gjae
    LEFT JOIN {{ ref('GeneralJournalEntry')  }} gje
      ON gje.RecId = gjae.GeneralJournalEntry
    LEFT JOIN {{ ref('Ledger')  }} Ledger
      ON Ledger.RecId = gje.Ledger
    LEFT JOIN {{ ref('FiscalCalendarPeriod')  }} fcp
      ON fcp.RecId = gje.FiscalCalendarPeriod
    left join {{ ref('DimensionAttributeValueCombination')  }} AS davc 
      ON gjae.LedgerDimension = davc.RECID
    /* Der kan være flere bilagsrækker til et bilag, så vi vælger den seneste række pr. bilag for ikke at få dubletter i facten */  
    LEFT JOIN ( SELECT voucher, dataareaid, ROW_NUMBER() OVER (PARTITION BY voucher, DataAreaId ORDER BY SinkModifiedOn DESC) AS rk_nr
	              FROM   {{ ref('VendTrans')  }}
				        WHERE  dbt_valid_to is null
			        ) vt
			        ON vt.voucher = gje.SubledgerVoucher
              AND vt.DataAreaId = Ledger.Name
			        AND vt.rk_nr = 1	
    LEFT JOIN {{ ref('LedgerTransVoucherLink')  }} ltvl 
	    ON ltvl.Voucher = gje.SubledgerVoucher
	    AND ltvl.dataareaid = Ledger.Name
	    AND ltvl.voucher like 'K-%'
	  LEFT JOIN {{ ref('LedgerTransVoucherLink')  }} vg_ltvl 
	    ON vg_ltvl.vouchergroupid = ltvl.vouchergroupid
      AND vg_ltvl.dataareaid = ltvl.dataareaid
	    AND vg_ltvl.recversion = 1
      AND vg_ltvl.voucher like 'K-%' 
    where gjae.dbt_valid_to is null 
    and gje.dbt_valid_to is null
    and Ledger.dbt_valid_to is null
    and fcp.dbt_valid_to is null
    and davc.dbt_valid_to is null
    and ltvl.dbt_valid_to is null 
	  and vg_ltvl.dbt_valid_to is null

)
,

Projekt as (
  
	SELECT DimprojektId
        ,ProjektId
        ,ProjektkontraktId
	FROM {{ ref('DimProjekt') }} 

)

,

/*
Vi laver en distict på få værdier da tabellen kun skal bruges som bridge over til taxtrans
*/
Momsbridge as 
(
  select distinct 
    GeneralJournalAccountEntry, TaxTransRelationship, DataAreaId, TaxTrans
  from {{ ref('TaxTransGeneralJournalAccountEntry') }}
  where dbt_valid_to is null
)
,
Moms as (
     SELECT
			ttgjae.GeneralJournalAccountEntry
          ,Max(ttgjae.TaxTransRelationship) as TaxTransRelationship
          ,Max(ttgjae.DataAreaId) as DataAreaId 
          ,max(tt.JournalNum) as JournalNum
          ,max(tt.Voucher) as Voucher
          ,cast(sum(tt.TaxBaseAmountRep) as decimal(32,16)) as TaxBaseAmountRep
          ,max(tt.TransDate) as TransDate
          ,cast(sum(tt.TaxValue) as decimal(32,16)) as Moms
          ,max(tt.TaxCode) as Momskode
          ,max(tt.TaxDirection) as Momsretning
          ,max(tt.TaxGroup) as Momsgruppe
          ,cast(sum(tt.TaxAmountRep) as decimal(32,16)) as Momsbeloeb
          ,cast(sum(tt.TaxInCostPriceRep) as decimal(32,16)) as IFM 
          ,max(tt.TaxItemGroup) as Varemomsgruppe
          ,max(tt.CurrencyCode) as MomsValutakode

    FROM Momsbridge as ttgjae
    INNER JOIN {{ ref('TaxTrans') }} as tt
	    ON tt.RECID = ttgjae.TaxTrans
	    AND tt.DataAreaId = ttgjae.DataAreaId
    where tt.dbt_valid_to is null 
    and tt.taxcode <> 'Ingen moms'
	  Group by ttgjae.GeneralJournalAccountEntry

)

select 
       case when gjae.FinTag = -1 
          then  {{ generate_Forca_surrogate_key( 'FinTag, null' ) }} 
          else {{ generate_Forca_surrogate_key( 'FinTag, Selskab' ) }} 
       end                                                            As DimFinanskoderId
      ,{{ generate_Forca_surrogate_key( 'MainAccount' ) }}            As DimFinanskontoId
      ,{{ generate_Forca_surrogate_key( 'ValutaKode' ) }}             As DimValutaId 
      ,{{ generate_Forca_surrogate_key( 'Bogfoeringstype' ) }}        As DimBogfoeringstypeId 
      ,{{ generate_Forca_surrogate_key( 'Posteringstype' ) }}         As DimFinansPosteringstypeId
      ,{{ generate_Forca_surrogate_key( 'Posteringsperiodetype' ) }}  As DimFinansPosteringsPeriodetypeId     
      ,{{ generate_Forca_surrogate_key( 'AFDELINGVALUE' ) }}          As DimAfdelingId     
      ,{{ generate_Forca_surrogate_key( 'COSTCENTERVALUE' ) }}        As DimBaererId 
      ,{{ generate_Forca_surrogate_key( 'ProjektId' ) }}              As DimProjektId  
      ,{{ generate_Forca_surrogate_key( 'Selskab' ) }}                As DimJuridiskEnhedId  
      ,{{ generate_Forca_surrogate_key( 'Momskode' ) }}               as DimMomskodeId
      ,{{ generate_Forca_surrogate_key( 'Momsretning' ) }}            as DimMomsretningId 
      ,{{ generate_Forca_surrogate_key( 'Momsgruppe' ) }}             as DimMomsgruppeId 
      ,{{ generate_Forca_surrogate_key( 'Varemomsgruppe' ) }}         as DimVaremomsgruppeId 
      ,case when Selskab != '1000' /*Andre selskaber end Forca ligger ikke i hierarkiet*/
          then {{ generate_Forca_surrogate_key( '-1' ) }} 
          else {{ generate_Forca_surrogate_key( 'AFDELINGVALUE' ) }} 
       end                                                            As DimForcaAfdelingshierarkiId
      ,{{ generate_Forca_surrogate_key( 'MainAccount' ) }}            As DimFinansSumkontoId
      ,{{ generate_Forca_surrogate_key( 'KreditorBilagsnummer, KreditorSelskab' ) }} As DimKreditorId 
      ,gjae.Bogfoeringsdato as DimBogfoeringsDatoId
      ,gjae.RECID
      ,gjae.FinansKonto
      ,gjae.Posteringsbeloeb
      ,gjae.Valutaposteringsbeloeb
      ,gjae.Posteringstekst
      ,gjae.ErKredit
      ,gjae.ErKreditTekst
      ,gjae.ErKorrektion    
      ,gjae.ErKorrektionTekst  
      ,gjae.PosteretAf
      ,gjae.Kladdenummer
      ,gjae.Bilagsnummer
      ,gjae.Selskab
      ,gjae.SCADeepLink                                                 as LinkTilBilag
      ,gjae.SCASourceEntryId                                            as KildeindtastningsId
      ,moms.Momsbeloeb
      ,moms.IFM 
      ,moms.Moms
      ,moms.MomsValutakode  
      ,gjae.Posteringsdato
      ,gjae.Dokumentdato
from GeneralJournalAccountEntry gjae 
left join Moms 
  on moms.GeneralJournalAccountEntry = gjae.RECID and TaxTransRelationship = 2 /* Vi vil kun finde moms information på grund beløbet */
left join Projekt pj
	on pj.ProjektId = gjae.PROJEKTVALUE