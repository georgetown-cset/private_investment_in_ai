/* This querry collects MA transaction from Crunchbase */
/* get the parent ifnroamtion of the target */

/* Save the results of the querry to create CB_MA_All.csv */


SELECT distinct * except(Crunchbase_ID_target, cb_url, uuid, parent_uuid, par_uuid1, ipo_org_uuid, went_public_on),
         COALESCE(parent_uuid, par_uuid1) AS parent_uuid,
         COALESCE(Crunchbase_ID_target, cb_url) AS Crunchbase_ID_target
FROM
    (SELECT *
   FROM
       (SELECT *
      FROM
          (SELECT * except(COMPANY_ID1)
         FROM
             (SELECT * except(Crunchbase_ID_target, Crunchbase_ID),
                       COALESCE(Crunchbase_ID_target, Crunchbase_ID) AS Crunchbase_ID_target
            FROM
                (SELECT * except(uuid)
               FROM
                   (SELECT distinct *
                  FROM
      /* get identity of the firms involved in MA deal */
                      (SELECT  uuid as ma_uuid, EXTRACT(YEAR
                                      FROM acquired_on) AS year,
                              acquiree_country_code AS target_country,
                              acquiree_name AS target_name,
                              acquiree_region AS target_province,
                              acquiree_uuid AS target_uuid,
                              acquirer_country_code AS investor_country,
                              acquirer_name AS investor_name,
                              acquirer_region AS investor_province,
                              acquirer_uuid AS investor_uuid,
                              acquired_on,
                              CAST(price_usd AS FLOAT64) AS investment_value,
                              CAST(price_usd AS FLOAT64) AS target_valuation
                     FROM private_ai_investment.acquisitions_latest) aq
                  left join
/* Check if the target had an IPO, if it had IPO before the MA deal, then delete it.
We are looking only for private companies */
                    ( SELECT org_uuid AS ipo_org_uuid,
                             went_public_on
                     FROM `gcp-cset-projects.private_ai_investment.ipos_latest`) ipos
                    ON target_uuid = ipo_org_uuid
                  WHERE went_public_on is Null
                    OR went_public_on < acquired_on ) ma
               LEFT JOIN
/* Get the type of the investor inforamation */
                 (SELECT uuid,
                         status AS investor_status,
                         cb_url AS Crunchbase_ID_target
                  FROM private_ai_investment.organizations_latest) pub
                 ON ma.target_uuid = pub.uuid) crunch
            LEFT JOIN
/* Get PermID and Company ID for later match with Refinitiv */
              (SELECT distinct CAST(PermID AS string) PermID_cr,
                               Crunchbase_ID,
                               COMPANY_ID
               FROM `gcp-cset-projects.private_ai_investment.CB_Ref_match_latest`
               WHERE COMPANY_ID is not NULL
                 AND Crunchbase_ID is not NULL) CR_merge
              ON crunch.Crunchbase_ID_target = CR_merge.Crunchbase_ID) CB /* add company country */
         LEFT JOIN
/* Assign country from Refinitiv data if it available, as this data is more reliable than CB */
           (SELECT COMPANY_ID AS COMPANY_ID1,
                   NATION AS COUNTRY_Ref
            FROM `gcp-cset-projects.private_ai_investment.Target_latest`) Ref
           ON CB.COMPANY_ID = Ref.COMPANY_ID1
/* drop years befreo 2013 */
         WHERE year > 2012) ma
/* get organization information about investors */
      LEFT JOIN
        (SELECT uuid,
                cb_url
         FROM private_ai_investment.organizations_latest) pub
        ON ma.investor_uuid = pub.uuid) inv
   left JOIN
/* find the parent of an investor */
     (SELECT parent_uuid,
             uuid AS child_uuid
      FROM private_ai_investment.org_parents_latest) parent
     ON inv.investor_uuid = parent.child_uuid) Invest1
LEFT JOIN
/* Use parent country is available to identify the organization */
  (SELECT uuid AS par_uuid1,
          country_code AS parent_country,
          name AS inv_parent_name
   FROM private_ai_investment.organizations_latest) par_org
  ON Invest1.parent_uuid = par_org.par_uuid1
WHERE target_uuid not in
         (SELECT org_uuid
          FROM `gcp-cset-projects.private_ai_investment.ipos_latest`)
       OR target_uuid is Null