/* Function for the regular expression company search shared_functions.isAICompany */
/* Match companies between Crunchbase and Refinitiv */
select distinct * from (
SELECT COALESCE(CB_UUID, uuid_cb) AS uuid_cb, * except(CB_UUID, uuid_cb, COMPANY_ID_fill1, Name_fill1, WEBSITE_fill1, WEBSITE_Ref, description_fill1,description_Ref,
    country_code, COUNTRY_CB, country_cb1, Name_Ref, COUNTRY_Ref1, COUNTRY_Ref),
/* fill missing variables from both datasets */
         COALESCE(Name_fill1, Name_Ref) AS Name_Ref,
         COALESCE(WEBSITE_fill1, WEBSITE_Ref) AS WEBSITE_Ref,
         COALESCE(description_fill1, description_Ref) AS description_Ref,
         COALESCE(COUNTRY_Ref1, COUNTRY_Ref) as COUNTRY_Ref, COALESCE(COUNTRY_CB, country_cb1, country_code) as COUNTRY_CB


FROM
/* Fill missing variable values */
    ( SELECT * except(description_CB, description_CB_c, uuid, uuid_c,Name_CB,crunch_name, Webpage_CB, homepage_url,
    crunch_id, COMPANY_NAME, description, TMTOrganizationPermID,WEBSITE,primary_role),
               COALESCE(uuid, uuid_c, CB_UUID) AS uuid_CB,
               COALESCE(Webpage_CB, homepage_url) AS Webpage_CB,
               COALESCE(Name_CB,crunch_name) AS Name_CB,
               COALESCE(description_CB, description_CB_c) AS description_CB,
   FROM
     (SELECT *
/* identify AI companies from Crunchbase */
      FROM
          (SELECT distinct * except(Crunchbase_ID, Crunchbase_ID_cb, description, homepage_url,PermID_cr, crunch_id, crunch_name, PermID_cb),
                             homepage_url AS Webpage_CB,
                             COALESCE(Crunchbase_ID,crunch_id, Crunchbase_ID_cb) AS Crunchbase_ID_CB,
                             COALESCE(PermID_cr,PermID_cb) AS PermID_CB,
                             crunch_name AS Name_CB,
                             description AS description_CB,
                             1 AS CB_AI
         FROM
             (SELECT *
            FROM
                (SELECT * except(primary_role,total_funding_usd)
               FROM
                 (SELECT * except(long_uuid, description, short_description),
                           CONCAT(short_description, " ", description) AS description
                  FROM
                      (SELECT uuid,
                              short_description,
                              total_funding_usd,
                              primary_role,
                              homepage_url,
                              country_code as country_cb1,
                              cb_url AS crunch_id,
                              name AS crunch_name
                     FROM private_ai_investment.organizations_latest) short
                  inner join
                    (SELECT uuid AS long_uuid,
                            description
                     FROM private_ai_investment.organization_descriptions_latest) long
                    ON short.uuid = long.long_uuid)--begin regex condition

               WHERE total_funding_usd is not NULL
                 AND primary_role='company'
                 AND shared_functions.isAICompany(description))  AI_crunch
/* merge with PermIDs and CompanyIDs to match with Refinitiv */
            LEFT JOIN
              (SELECT Crunchbase_ID AS Crunchbase_ID_cb,
                      CAST(PermID AS string) AS PermID_cb, CB_UUID
               FROM `gcp-cset-projects.private_ai_investment.CB_Ref_match_latest`
               WHERE PermID is not NULL
                 AND Crunchbase_ID is not NULL) cea
              ON AI_crunch.crunch_id = cea.Crunchbase_ID_cb) crunch
         LEFT JOIN
           (SELECT distinct CAST(PermID AS string) PermID_cr,
                            Crunchbase_ID,
                            COMPANY_ID AS COMPANY_ID_CB
            FROM `gcp-cset-projects.private_ai_investment.CB_Ref_match_latest`
            WHERE COMPANY_ID is not NULL
              AND Crunchbase_ID is not NULL) CR_merge
           ON crunch.Crunchbase_ID_cb = CR_merge.Crunchbase_ID) cb /* NOW MATCH WITH REF */
      full join
    /* JOIN WITH REFINITIV */
        (SELECT distinct * except(Crunchbase_ID,COMPANY_ID_cr, COMPANY_ID_Ref,WEBSITE, ANAME, PermID,OrganizationID, PermID_cr, Crunchbase_ID_m),
                           WEBSITE,
                           COALESCE(COMPANY_ID_cr, COMPANY_ID_Ref) AS COMPANY_ID_Ref,
                           COALESCE(Crunchbase_ID_m, Crunchbase_ID) AS Crunchbase_ID_Ref,
                           COALESCE(PermID_cr,OrganizationID) AS PermID_Ref,
                           COMPANY_NAME AS Name_Ref,
                           description AS description_ref,
                           WEBSITE AS Website_Ref,
                           1 AS Ref_AI
         FROM
           (SELECT * except(Crunchbase_ID),
                     COALESCE(Crunchbase_ID_m, Crunchbase_ID) AS Crunchbase_ID
            FROM
                (SELECT *
               FROM
                   (SELECT *
                  FROM
                      (SELECT *
                     FROM
/* Get data from Refiniitv */
                       (SELECT PERMID,
                               COMPANY_ID AS COMPANY_ID_Ref,
                               COMPANY_NAME,
                               ANAME,
                               WEBSITE,NATION as COUNTRY_Ref,
                               CONCAT(BUSDESC_SHORT, " ", BUSINESS_DESC) AS description
                        FROM private_ai_investment.Target_latest)
                     WHERE shared_functions.isAICompany(description)) Ref
                  LEFT JOIN
/* Match with PermID for later match with Crunchbase */
                    (SELECT OrganizationID,
                            TMTOrganizationPermID
                     FROM `gcp-cset-projects.private_ai_investment.PERMID_latest`) permid_tab
                    ON Ref.PermID = permid_tab.TMTOrganizationPermID) Ref2
               LEFT JOIN
                 (SELECT distinct CAST(PermID AS string) PermID_cr,
                                  Crunchbase_ID,
                  FROM `gcp-cset-projects.private_ai_investment.CB_Ref_match_latest`
                  WHERE PermID is not NULL
                    AND Crunchbase_ID is not NULL) CR_merge
                 ON Ref2.OrganizationID = CR_merge.PermID_cr) ref3
            LEFT JOIN
/* Match with a table that links CB and Refinitiv companies */
              (SELECT distinct COMPANY_ID AS COMPANY_ID_cr,
                               Crunchbase_ID AS Crunchbase_ID_m,
               FROM `gcp-cset-projects.private_ai_investment.CB_Ref_match_latest`
               WHERE COMPANY_ID is not NULL
                 AND Crunchbase_ID is not NULL) CR_merge
              ON Ref3.COMPANY_ID_Ref = CR_merge.COMPANY_ID_cr)) ref
        ON cb.Crunchbase_ID_CB = ref.Crunchbase_ID_Ref) main
   left JOIN (
/* merge long and short descriptions */
                (SELECT * except(long_uuid, description, short_description, uuid),
                          CONCAT(short_description, " ", description) AS description_CB_c,
                          uuid AS uuid_c
                 FROM
/* Get organizational information from Crunchbase */
                     (SELECT short_description,
                             primary_role,
                             uuid,
                             homepage_url,
                             country_code,
                             cb_url AS crunch_id, country_code as COUNTRY_CB,
                             name AS crunch_name
                    FROM private_ai_investment.organizations_latest) short
                 inner join
                   (SELECT uuid AS long_uuid,
                           description
                    FROM private_ai_investment.organization_descriptions_latest) long
                   ON short.uuid = long.long_uuid)) CB
     ON main.Crunchbase_ID_Ref = CB.crunch_id) main
/* Get organizational information from Refinitiv */
LEFT JOIN
  (SELECT COMPANY_ID AS COMPANY_ID_fill1,
          COMPANY_NAME AS Name_fill1,
          WEBSITE AS WEBSITE_fill1,WEBSITE,NATION as COUNTRY_Ref1,
          CONCAT(BUSDESC_SHORT, " ", BUSINESS_DESC) AS description_fill1
   FROM private_ai_investment.Target_latest) T
     ON main.COMPANY_ID_CB = T.COMPANY_ID_fill1)

