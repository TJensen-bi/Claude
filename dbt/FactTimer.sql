WITH ProjEmplTrans AS (

    {# Transaktionstype er hardcodet, da der i nogle tilfælde kan mangle en relation til ProjTransPosting, hvis der ikke er salgs eller omkostningsværdi på transaktionen #}
    SELECT 
         p.RECID
        ,TransId
        ,p.ProjId
        ,p.DataAreaId
        ,ActivityNumber
        ,CategoryId
        ,DefaultDimension
        ,Qty
        ,LinePropertyId
        ,[resource]  
        ,SCACreatedFromDistribution
        ,SCADistributed
        ,SCAProjDistributionTransId
        ,TaxGroupId as MomsgruppeId
        ,TaxItemGroupId as VaremomsgruppeId
        ,TransactionOrigin
        ,TransDate
        ,VoucherJournal
        ,TotalCostAmountCur
        ,TotalSalesAmountCur 
        ,Txt      
        ,2 as TransType
        ,ISNULL(s.ProjId,'-1') AS PMOProjId   
        ,TaxGroupId
        ,TaxItemGroupId  
    FROM {{ ref( 'ProjEmplTrans')  }} p
    LEFT JOIN {{ ref( 'SCAProjPMOInfomationsFields')  }} s /*Ikke alle ProjId er i PMO modul*/
    ON s.ProjId = p.ProjId
    where p.dbt_valid_to is null
    and s.dbt_valid_to is null
    and p.projID not in ('P000001', 'P999998')  -- fravær og langtidsfravær
),

DimensionAttributeValueCombination AS (

    SELECT 
          RECID
         ,MainAccount
         ,MainAccountValue
    FROM {{ ref( 'DimensionAttributeValueCombination')}}
    where dbt_valid_to is null
),

DimensionAttributeValueSet AS (

    SELECT 
          RECID
         ,AFDELINGVALUE
         ,COSTCENTERVALUE
    FROM {{ ref( 'DimensionAttributeValueSet')}}
    where dbt_valid_to is null
),

ProjTable AS (

    SELECT 
         ProjId
        ,WorkerResponsible
        ,ProjInvoiceProjId
        ,DataAreaId
        ,RECID
        ,dbt_valid_to
        /* Skal bruges til at finde satser */
        ,custAccount as DebitorKonto
        ,SCAProjCostAllocationServiceType as YdelsestypeId
        ,SCAPaymentTansaction as BetalingstransaktionId
        ,SCAManagementService as ForvaltningsydelseId
    FROM {{ ref( 'ProjTable')  }} 
    where dbt_valid_to is null
),

ProjTransPosting AS (

    SELECT 
          TransId
         ,ProjTransType 
         ,PostingType
         ,LedgerTransDate
         ,LedgerDimension
         ,DataAreaId
         ,Voucher
    FROM {{ ref( 'ProjTransPosting')  }} 
    where dbt_valid_to is null
    AND ProjTransType = 2 -- 'Hour'
    AND PostingType IN (121, 123, 134) --ProjCost, ProjWIPCostvalue, ProjNeverLedger (Reguleret)
    AND LedgerOrigin NOT IN (57, 58) -- EliminateEstimate, ReverseElimination
    group by TransId, ProjTransType, PostingType, LedgerTransDate, LedgerDimension, DataAreaId, Voucher
),

MomsDims as (
  select Voucher
  ,TransDate
  ,TaxCode as Momskode
  ,[TaxDirection] as Momsretning
  ,[TaxGroup] as Momsgruppe
  ,[TaxItemGroup] as Varemomsgruppe
  FROM {{ ref( 'TaxTrans')  }}
  where dbt_valid_to is null

),

/* Find den rå kostpris pr. transaktion */
ProjEmplTransCost as (

    SELECT  CostPrice
           ,TransId
           ,DataAreaId
    FROM {{ ref( 'ProjEmplTransCost')  }}
  where dbt_valid_to is null

),

/* Find den rå salgspris pr. transaktion */
ProjEmplTransSale as (

    SELECT  SalesPrice
           ,TransId
           ,DataAreaId
           ,LedgerTransdate
    FROM {{ ref( 'ProjEmplTransSale')  }}
  where dbt_valid_to is null

),

MedarbejderLink as (
    select 
        rri.RECID as TimerResourceLink
        ,wor.PersonnelNumber MedarbejderId365
    From {{ ref('ResResourceIdentifier')  }} rri
    Left Join {{ ref( 'WrkCtrTable')  }} wct on rri.RefRecId = wct.RECID
    left join {{ ref( 'HcmWorker')  }} wor on wct.Worker = wor.recid
    where 
        rri.dbt_valid_to is null and wct.dbt_valid_to is null and wor.dbt_valid_to is null
        and rri.RefTableId = 11989  -- WrkCtrTable

),

MedarbejderAfdelingHistorik as (
    Select distinct 
        worker.PersonnelNumber
        ,CAST(CAST(empl.ValidFrom AS DATETIMEOFFSET) AT TIME ZONE 'Central European Standard Time' AS date) ValidFrom
        , CAST(CAST(ValidTo AS DATETIMEOFFSET) AT TIME ZONE 'Central European Standard Time' AS date) ValidTo 
        , dvs.AFDELINGVALUE
    from {{ ref( 'HcmWorker')  }} worker 
    inner join {{ ref( 'HcmEmployment')  }} empl on worker.RECID = empl.Worker
    inner join {{ ref( 'DimensionAttributeValueSet')  }} dvs on empl.DefaultDimension = dvs.RECID
    where worker.dbt_valid_to is null
    and empl.dbt_valid_to is null
    and dvs.dbt_valid_to is null
),

TransactionOrigin AS (

    SELECT 
       EnumNr
      ,EnumValue
  FROM {{ ref('D365FOEnumValuesDK') }}
  where KildeTabel = 'ProjEmplTrans' and EnumType = 'TransactionOrigin'
),


/*Denne er oprettet for at kunne oprettet nøgler til Ukendt kolonner hvor de skal bruges dato som input*/
DummyDate AS (
    SELECT 
         {{ generate_unknown_value( 'date' ) }}     as NullDate
),


/* FAKTURASTATUS. Reglerne er taget fra X++ kode, der ligger i D365 */

-- Reguleret
Reguleret as (
	select  recid,
			TransId, 
			DataAreaId,
	        Fakturastatus = 'Reguleret'	 
	from {{ ref( 'ProjEmplTransSale')  }}  
	where TransStatus = 9 -- 'Adjusted'
    and dbt_valid_to is null

),

-- Faktureret
Faktureret as (

	select   pets.RECID
	        ,pets.TransId
			,pets.DataAreaId
			,pet.ProjId
	        ,Fakturastatus = 'Faktureret'
	from {{ ref( 'ProjEmplTransSale') }} pets
	inner join {{ ref( 'ProjEmplTrans') }} pet
		on pet.TransId = pets.TransId
	inner join {{ ref( 'ProjLineProperty') }} plp
		on plp.LinePropertyId = pet.LinePropertyId
		and plp.DataAreaId = pet.DataAreaId
	where plp.ToBeInvoiced = 1 -- 'Yes'
	and (pets.TransStatus = 4 -- 'Invoiced'
	or pets.TransStatus = 5 -- 'MarkedCreditnote' -- bruges ikke
	or pets.TransStatus = 6 -- 'CreditnoteProposal' -- bruges ikke
    )
    and pets.dbt_valid_to is null
    and pet.dbt_valid_to is null
    and plp.dbt_valid_to is null

),


OnHoldFunding as (

-- OnHoldFunding
	select   pets.RECID
	        ,pets.TransId
	        ,pets.DataAreaId
			,pet.ProjId
	        ,Fakturastatus = 'OnHoldFunding'
	from {{ ref('ProjEmplTransSale') }} pets
	inner join {{ ref('ProjEmplTrans') }} pet
		on pet.TransId = pets.TransId
	inner join {{ ref('ProjLineProperty') }} plp
		on plp.LinePropertyId = pet.LinePropertyId
	inner join {{ ref('ProjFundingSource') }} pfs
		on pfs.recid = pets.FundingSource
	where plp.ToBeInvoiced = 1 -- 'Yes'
	and (pets.TransStatus =  4 -- 'Invoiced'
	or pets.TransStatus = 5 -- 'MarkedCreditnote' -- bruges ikke
	or pets.TransStatus = 6 -- 'CreditnoteProposal' -- bruges ikke
    )
	and pfs.FundingType = 3 --'OnHoldFundingSource'
    and pets.dbt_valid_to is null
    and pet.dbt_valid_to is null
    and plp.dbt_valid_to is null
    and pfs.dbt_valid_to is null
),

Fakturerbar as (
-- Fakturerbar
	select   pets.RECID
			,pets.TransId
			,pets.DataAreaId
	        ,Fakturastatus = 'Fakturebar'
	from {{ ref('ProjEmplTransSale') }} pets
	inner join {{ ref('ProjFundingSource') }} pfs
		on pfs.recid = pets.FundingSource
	where (pets.TransStatus = 2 /*'Posted'*/ or pets.TransStatus = 3 /* 'InvoiceProposal' */)
	and (pfs.FundingType = 0 /*'Customer' */ or pfs.FundingType = 2 /*'Grant' */)
    and pets.dbt_valid_to is null
    and pfs.dbt_valid_to is null
)
,

Fakturastatus AS (

	select a.RECID
	      ,a.TransId
		  ,a.DataAreaId
	     ,case when ohf.recid is not null then 'PartiallyInvoicedWithOnhold' else 'PartiallyInvoiced' end as Fakturastatus	        
	from Faktureret a
	inner join Fakturerbar b
		on b.RECID = a.RECID
	left join OnHoldFunding ohf
		on ohf.RECID = a.RECID
	
	union 
	
	select a.RECID
	      ,a.TransId
		  ,a.DataAreaId
	     ,case when ohf.recid is not null then 'FullyInvoicedWithOnhold' else 'Fuldt ud faktureret' end as Fakturastatus	        
	from Faktureret a
	left join Fakturerbar b
		on b.RECID = a.RECID
	left join OnHoldFunding ohf
		on ohf.RECID = a.RECID
	where b.RECID is null
	
	union
	
	select a.RECID
	      ,a.TransId
		  ,a.DataAreaId
	     ,'Fakturerbar' as Fakturastatus	        
	from Fakturerbar  a
	left join  Faktureret b
		on b.RECID = a.RECID
	where b.RECID is null
	
	union 
	
	select a.RECID
	      ,a.TransId
		  ,a.DataAreaId
	      ,a.Fakturastatus	        
	from Reguleret  a

)


SELECT 
     {{ generate_Forca_surrogate_key( 'pet.ProjId' ) }}                     AS  DimProjektId
    ,{{ generate_Forca_surrogate_key( 'pet.PMOProjId' ) }}                  AS  DimPMOStamdataId
    ,{{ generate_Forca_surrogate_key( 'pet.CategoryId, pet.DataAreaId' ) }} AS  DimKategoriId
    ,{{ generate_Forca_surrogate_key( 'pet.ActivityNumber' ) }}             AS  DimWBSId
    ,case when pet.DataAreaId != '1000' /*Andre selskaber end Forca ligger ikke i hierarkiet*/
           then {{ generate_Forca_surrogate_key( '-1' ) }} 
           else {{ generate_Forca_surrogate_key( 'da.AFDELINGVALUE' ) }} 
          end                                                               AS  DimForcaAfdelingshierarkiId
    ,{{ generate_Forca_surrogate_key( 'da.AFDELINGVALUE' ) }}               AS  DimAfdelingId
    ,{{ generate_Forca_surrogate_key( 'afdHist.AFDELINGVALUE' ) }}          AS  DimMedarbejderForcaAfdelingshierarkiId
    ,{{ generate_Forca_surrogate_key( 'afdHist.AFDELINGVALUE' ) }}          AS  DimMedarbejderAfdelingId 

    ,{{ generate_Forca_surrogate_key( 'pet.TaxGroupId' ) }}                 AS  DimMomsgruppeId
    ,{{ generate_Forca_surrogate_key( 'pet.TaxItemGroupId' ) }}             AS  DimVaremomsgruppeId
    -- d. 10.06.2024 Moms udkommenteret for nu. Se INC37808
{#  ,{{ generate_Forca_surrogate_key( 'moms.Momsretning' ) }}               AS  DimMomsretningId
    ,{{ generate_Forca_surrogate_key( 'moms.Momskode' ) }}                  AS  DimMomskodeId #}
    ,{{ generate_Forca_surrogate_key( '-1' ) }}                               AS  DimMomsretningId
    ,{{ generate_Forca_surrogate_key( '-1' ) }}                               AS  DimMomskodeId

    ,pet.TransDate                                                          AS  DimProjektDatoId
    ,ISNULL(NULLIF(ptp.LedgerTransDate,''),pets.LedgerTransDate)            AS  DimFinansDatoId
    ,iif(andel.DimKundeandelId is not null, andel.DimKundeandelId, 
           {{ generate_Forca_surrogate_key( 'andel.ProjektId, DummyDate.NullDate' ) }}  ) AS  DimKundeandelId
    ,iif(sats.DimProjektsatsId is not null, sats.DimProjektsatsId, 
           {{ generate_Forca_surrogate_key( 'sats.DebitorKonto, sats.ForvaltningsydelseId, sats.BetalingstransaktionId, sats.YdelsestypeId, DummyDate.NullDate' ) }}  ) AS DimProjektsatsId
    ,{{ generate_Forca_surrogate_key( 'dav.MainAccount' ) }}                AS  DimFinanskontoId
    ,{{ generate_Forca_surrogate_key( 'pet.TransType' ) }}                  AS  DimTransaktionstypeId
    ,{{ generate_Forca_surrogate_key( 'ml.MedarbejderId365' ) }}           AS  DimMedarbejderId  
    ,ml.MedarbejderId365
    ,pet.RECID
    ,pet.ProjId  
    ,pet.TransId                                                            AS PosteringsId                                                          
    ,pet.Qty
    ,CASE WHEN pet.SCACreatedFromDistribution = 1 THEN 'Ja' ELSE 'Nej'  END  AS OprettetFraProjektfordeling
    ,CASE WHEN pet.SCADistributed = 1 THEN 'Ja' ELSE 'Nej'  END              AS Fordelt
    ,pet.SCAProjDistributionTransId                                          AS FordelingsId 
    ,petc.CostPrice * pet.Qty                                                AS TotalCostAmountDKK
    ,CASE WHEN pet.LinePropertyId = 'FAK' 
        THEN pets.SalesPrice * pet.Qty 
        ELSE NULL
     END                                                                    AS TotalSalesAmountDKK 
    ,pet.DataAreaId
    ,petc.CostPrice                                                         AS Kostpris 
    ,pets.SalesPrice                                                        AS Salgspris 
    ,pet.VoucherJournal                                                     AS Bilagsnummer
    ,pet.LinePropertyId                                                     AS Linjeegenskab
    ,tao.EnumValue                                                          AS Posteringsgrundlag
    ,pet.txt                                                                AS Beskrivelse 
    ,CASE WHEN fs.Fakturastatus = 'Reguleret' THEN fs.Fakturastatus
          ELSE 
          CASE WHEN pet.LinePropertyId = 'FAK' and fs.Fakturastatus is not null THEN fs.Fakturastatus
          ELSE 'Kan ikke faktureres'
          END 
     END                                                                    AS Fakturastatus
    
FROM ProjEmplTrans pet
LEFT JOIN ProjTable pt 
    ON pet.ProjId = pt.ProjId
    AND pet.DataAreaId = pt.DataAreaId
LEFT JOIN DimensionAttributeValueSet da
    ON pet.DefaultDimension = da.RECID
CROSS JOIN DummyDate
LEFT JOIN {{ref('DimKundeandel')}} andel 
    ON pet.ProjId = andel.ProjektId 
    AND pet.TransDate BETWEEN andel.GyldigFra AND andel.Udloeb
LEFT JOIN {{ref('DimProjektsatser')}} sats 
    ON pt.DebitorKonto = sats.DebitorKonto
    AND pt.YdelsestypeId = sats.YdelsestypeId
    AND pt.BetalingstransaktionId = sats.BetalingstransaktionId
    AND pt.ForvaltningsydelseId = sats.ForvaltningsydelseId
    AND pet.TransDate BETWEEN sats.GyldigFra AND sats.Udloeb
LEFT JOIN ProjTransPosting ptp
    ON pet.TransId = ptp.TransId
    AND ptp.DataAreaId = pet.DataAreaId
    AND pet.VoucherJournal   = ptp.Voucher
LEFT JOIN DimensionAttributeValueCombination dav
    ON ptp.LedgerDimension = dav.RECID
-- d. 10.06.2024 Moms udkommenteret for nu. Se INC37808
{# LEFT JOIN MomsDims moms 
    ON pet.VoucherJournal = moms.Voucher 
    AND pet.TransDate = moms.TransDate #}
LEFT JOIN ProjEmplTransCost petc 
    ON petc.TransId = pet.TransId
    AND petc.DataAreaId = pet.DataAreaId
LEFT JOIN ProjEmplTransSale pets 
    ON pets.TransId = pet.TransId
    AND pets.DataAreaId = pet.DataAreaId
LEFT JOIN TransactionOrigin tao 
    ON tao.EnumNr = pet.TransactionOrigin
LEFT JOIN MedarbejderLink ml 
    ON pet.[resource] = ml.TimerResourceLink 
LEFT JOIN MedarbejderAfdelingHistorik afdHist 
    on ml.MedarbejderId365 = afdHist.PersonnelNumber and pet.TransDate  between afdHist.ValidFrom and afdHist.ValidTo
LEFT JOIN Fakturastatus fs 
    ON fs.TransId = pets.TransId
    AND fs.DataAreaId = pets.DataAreaId
