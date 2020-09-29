/* Save the results of the querry to create Ref_MA.csv */

/* Get All MA Deals from Refinitiv. MASTER_DEAL_NO is the main ID */
SELECT distinct * except(DEAL_NO, T_OA_PERMID)
FROM
  (SELECT MASTER_DEAL_NO, APUBLIC , TPUBLIC, TUP,
          EXTRACT(YEAR
                  FROM DATEANN) AS year,
          VALUE
   FROM `gcp-cset-projects.private_ai_investment.MA_latest`
      /* Keep only completed transatsions where the investor is public*/
   WHERE DATEANN is not NULL AND STATUS = 'Completed'
   ) deals
INNER JOIN
  (SELECT distinct *
   FROM
       (SELECT distinct MASTER_DEAL_NO as deal_No,
                        CAST(T_OA_PERMID AS string) AS T_OA_PERMID
      FROM `gcp-cset-projects.private_ai_investment.MA_Org_latest`) MA_Org
   INNER JOIN
     (SELECT TMTOrganizationPermID,
             OrganizationID
      FROM `gcp-cset-projects.private_ai_investment.PERMID_latest`) Perm
     ON MA_Org.T_OA_PERMID = Perm.OrganizationID) ids ON deals.MASTER_DEAL_NO = ids.deal_No
     /* Drop if the target is public company */
     WHERE year > 2012 AND TPUBLIC != 'Public'

