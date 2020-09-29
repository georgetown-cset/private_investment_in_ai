/*
This querry created a list of AI companies from Refinitiv data searching for regular expression keywords
in the company description. We merge both short and long company description together.
shared_functions.isAICompany for regular expression
Save the results of the querry to create ai_companies_in_refinitiv.csv */
SELECT * except(cid) from (SELECT distinct CB_UUID, WEBSITE, COALESCE(COMPANY_ID_cr , COMPANY_ID) as COMPANY_ID,
COALESCE(Crunchbase_ID_m , Crunchbase_ID) as Crunchbase_ID , COALESCE(PermID_cr ,OrganizationID) as PermID ,
COMPANY_NAME as Name_Ref, description as description_ref, WEBSITE as Website_Ref
from(
select * except(Crunchbase_ID, CB_UUID, CB_UUID1) ,COALESCE(Crunchbase_ID_m , Crunchbase_ID) as Crunchbase_ID, COALESCE(CB_UUID, CB_UUID1)
as CB_UUID from (
select *  from
(
SELECT *
FROM
    (SELECT *
   FROM
     (SELECT PERMID,
             COMPANY_ID,
             COMPANY_NAME,
             ANAME, WEBSITE,
             CONCAT(BUSDESC_SHORT, " ", BUSINESS_DESC) AS description
      FROM private_ai_investment.Target_latest where PUBSTATUS != "P")
WHERE shared_functions.isAICompany(description)
) Ref
/* Below we match refintiv companies with Crunchbase databased using both PermID and CompanyID */
LEFT JOIN
     (SELECT OrganizationID,
             TMTOrganizationPermID, CommonName as PermID_common_name, LegalName as PermID_legal_name,
             ShortName as PermID_name,
      FROM `gcp-cset-projects.private_ai_investment.PERMID_latest`) permid_tab
  ON Ref.PermID = permid_tab.TMTOrganizationPermID ) Ref2
LEFT JOIN
       (SELECT distinct CAST(PermID AS string) PermID_cr,
               Crunchbase_ID, CB_UUID
      FROM `gcp-cset-projects.private_ai_investment.CB_Ref_match_latest`
      WHERE PermID is not NULL and  Crunchbase_ID is not NULL) CR_merge
  ON Ref2.OrganizationID = CR_merge.PermID_cr

  ) ref3
  LEFT JOIN
         (SELECT distinct COMPANY_ID as COMPANY_ID_cr,
               Crunchbase_ID as Crunchbase_ID_m, CB_UUID as CB_UUID1
      FROM `gcp-cset-projects.private_ai_investment.CB_Ref_match_latest`
      WHERE COMPANY_ID is not NULL and Crunchbase_ID is not NULL) CR_merge
  ON Ref3.COMPANY_ID = CR_merge.COMPANY_ID_cr)
) REF
LEFT JOIN
/* We merge companies with application codes constructed by Zach */
(select distinct CB_UUID as cid,  application_code from `gcp-cset-projects.private_ai_investment.applications_latest`) app
ON REF.CB_UUID = app.cid