/* This querry extracts AI companies from Crunchbase using regular expression querry applied to
organization description (merged long and short)
Function for the regular expression company search */
select distinct * except(CB_UUID,status, last_funding_on ) from (select * except(R_id) from (select * except(OrganizationID) from (SELECT distinct * except(Crunchbase_ID, Crunchbase_ID_cb, country_code,homepage_url,PermID_cr, crunch_id, crunch_name, PermID_cb) ,  homepage_url, COALESCE(Crunchbase_ID,crunch_id, Crunchbase_ID_cb) as Crunchbase_ID,  COALESCE(PermID_cr ,PermID_cb) as PermID ,
 crunch_name
from (SELECT *
FROM (
SELECT * except(primary_role,total_funding_usd)
FROM (
/* merge long and short description */
SELECT * except(long_uuid, description, short_description), CONCAT(short_description, " ", description) AS description
                FROM
/* get organization information to targets */
                    (SELECT uuid, short_description, total_funding_usd, status, last_funding_on , primary_role ,homepage_url , country_code  , cb_url AS crunch_id, name AS crunch_name
                   FROM private_ai_investment.organizations where uuid not in (SELECT org_uuid FROM `gcp-cset-projects.private_ai_investment.ipos_latest`)) short
                inner join
                  (SELECT uuid AS long_uuid, description
                   FROM private_ai_investment.organization_descriptions_latest) long
                  ON short.uuid = long.long_uuid)--begin regex condition
/* drop deals after the company was already aquired */
WHERE  primary_role='company' and NOT (last_funding_on is NULL and status != 'acquired')
  AND shared_functions.isAICompany(description)
) AI_crunch
  LEFT JOIN
/* merge with PermIDs for later match with Refinitiv */
    (SELECT Crunchbase_ID as Crunchbase_ID_cb,
            CAST(PermID as string) as PermID_cb,
     FROM `gcp-cset-projects.zach_companies.CB_Ref_match` WHERE PermID is not NULL
        AND Crunchbase_ID is not NULL) cea
    ON AI_crunch.crunch_id = cea.Crunchbase_ID_cb) crunch
 LEFT JOIN
       (SELECT distinct CAST(PermID AS string) PermID_cr,
               Crunchbase_ID, COMPANY_ID
      FROM  `gcp-cset-projects.private_ai_investment.CB_Ref_match_latest`
      WHERE COMPANY_ID is not NULL
        AND Crunchbase_ID is not NULL) CR_merge
  ON crunch.Crunchbase_ID_cb = CR_merge.Crunchbase_ID
  ) CB
/* Merge with PermID table to get Refinitiv country, which is more reliable than Crunchbase country */
  Left join (SELECT OrganizationID, CommonName as PermID_common_name, LegalName as PermID_legal_name,
             ShortName as PermID_name,
      FROM `gcp-cset-projects.private_ai_investment.PERMID_latest`) perm
      ON CB.PermID = perm.OrganizationID) cb
      left join      (SELECT
             COMPANY_ID as R_id, NATION as COUNTRY_Ref,
             COMPANY_NAME as name_ref, WEBSITE as website_ref,
             CONCAT(BUSDESC_SHORT, " ", BUSINESS_DESC) AS description_ref
      FROM private_ai_investment.Target_latest) t
ON cb.COMPANY_ID=  t.R_id) CB
/* match companies with applications data constructed by Zach */
LEFT JOIN
(select distinct CB_UUID,  application_code from `gcp-cset-projects.private_ai_investment.applications_latest`) app
ON CB.uuid = app.CB_UUID