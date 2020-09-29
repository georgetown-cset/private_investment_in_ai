/* Save the results of the querry to create All_REF_VC.csv */

/* Unique ID is the combination of Deal_NO, Round_No, Firm_ID*/

/* This querry creates a list of VC transactions from Refinitiv data.  */
Select distinct CONCAT(Deal_NO,"_", Round_NUM, "_", IF(Firm_ID is Null, 10000*RAND(),Firm_ID)) as deal_uuid, ANNDATE, COMPANY_ID, target_name, target_country, PUBSTATUS, year, ROUND_TOTAL, DEAL_NO, ROUND_NUM, investment_value, OrganizationID, TMTOrganizationPermID, PermID_name, LegalName, FIRM_IDm, FIRM_NAME, investor_country, Crunchbase_ID_m  from  (SELECT distinct *
FROM
    (SELECT * except(PermID)
   FROM
       (SELECT * except(COMPANY_ID_inv)
      FROM
/* This company information from Target table  */
          (SELECT ANNDATE,
                  COMPANY_ID,
                  PermID,
                  COMPANY_NAME AS target_name,
                  NATION AS target_country,
                  PUBSTATUS,
                  OFFER_PRiCE,
                  OFFER_SIZE,
                  VEIC6
         FROM private_ai_investment.Target_latest where PUBSTATUS != 'P' ) T
     RIGHT JOIN /* Investment detail */
   (SELECT * except(DEAL_NUMBER, ROUND_NUMBER)
   FROM
  /* Merge company data with individual investment details. We assign equal value to all investors
  becuase of many missing values by ROUND_TOTAL/count(ROUND_TOTAL), where ROUND_TOTAL is the total
  amount raised in VC round */
       (SELECT COMPANY_ID AS COMPANY_ID_inv,
               DISB_YEAR AS year,
               ROUND_TOTAL,
               DEAL_NO,
               ROUND_NUM,
               ROUND_TOTAL/count(ROUND_TOTAL) AS investment_value
      FROM private_ai_investment.INVDTL_latest
      GROUP BY COMPANY_ID,
               DISB_YEAR,
               ROUND_TOTAL,  DEAL_NO, ROUND_NUM, ROUND_TOTAL) i1
   LEFT JOIN
   /* merge deals with investors information to get FirmIDs (Investor IDs) */
     (SELECT DEAL_NO as Deal_number,
             ROUND_NUM as round_number,
             Firm_ID
      FROM private_ai_investment.INVDTL_latest) i2
     ON i1.DEAL_NO = i2.Deal_number
     AND i1.ROUND_NUM = i2.round_number) invd
     ON invd.COMPANY_ID_inv = T.COMPANY_ID/* Fund */ ) Ref
LEFT JOIN
/* Get  PermIDs for companies for later match with Crunchabase */
  (SELECT OrganizationID,
          TMTOrganizationPermID,
          ShortName AS PermID_name,
          LegalName
   FROM `gcp-cset-projects.private_ai_investment.PERMID_latest`) permid_tab
  ON Ref.PermID = permid_tab.TMTOrganizationPermID ) ref
LEFT JOIN
/* Get  PermIDs for investors for later match with Crunchabase */
  (SELECT FIRM_ID AS FIRM_IDm,
          FIRM_NAME,
          NATION AS investor_country
   FROM private_ai_investment.FIRM_latest) inv
  ON ref.FIRM_ID = inv.FIRM_IDm ) Ref3
  /* Get Crunchbase IDs for target companies */
    LEFT JOIN
         (SELECT distinct COMPANY_ID as COMPANY_ID_cr,
               Crunchbase_ID as Crunchbase_ID_m,
      FROM `gcp-cset-projects.private_ai_investment.CB_Ref_match_latest`
      WHERE COMPANY_ID is not NULL and Crunchbase_ID is not NULL) CR_merge
  ON Ref3.COMPANY_ID = CR_merge.COMPANY_ID_cr
  WHERE year > 2012

