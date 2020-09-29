
select distinct *  from
(
SELECT CONCAT(round_id, investor_uuid) as investment_uid , *
FROM
  (SELECT * except(COMPANY_ID1,ipo_org_uuid, went_public_on, investor_uuid), IF(investor_uuid is NULL, CONCAT('NULL:',GENERATE_UUID()), investor_uuid)  as investor_uuid
   FROM
    /* Merge company data with individual investment details. We assign equal value to all investors
  becuase of many missing values by ROUND_TOTAL/count(ROUND_TOTAL), where ROUND_TOTAL is the total
  amount raised in VC round */
       (SELECT *,
               ROUND_TOTAL/inv_count AS investment_value
      FROM
          (SELECT * except(inv_count1, inv_count),
                    coalesce(inv_count,inv_count1) AS inv_count
         FROM
             (SELECT * except(funding_round_uuid)
            FROM
                (SELECT distinct *
               FROM
/* Get deal inforamtion from funding round. */
                   (SELECT EXTRACT(YEAR
                                   FROM announced_on) AS year,
                           announced_on,
                           country_code AS target_country, uuid as round_id,
                           org_name AS target_name,
                           org_uuid AS target_uuid,
                           CAST(raised_amount_usd AS FLOAT64) AS ROUND_TOTAL,
                           CAST(post_money_valuation_usd AS FLOAT64) AS target_valuation,
                           uuid AS funding_round_uuid,
                           investment_type,
                           1 AS inv_count1
                  FROM private_ai_investment.funding_rounds_latest
/* We resrict to only VC-relevant deals */
                  WHERE investment_type IN ('series_a',
                                            'seed',
                                            'angel',
                                            'series_unknown',
                                            'private_equity',
                                            'series_d',
                                            'series_b',
                                            'convertible_note',
                                            'undisclosed',
                                            'pre_seed',
                                            'corporate_round',
                                            'series_c',
                                            'series_e',
                                            'series_i',
                                            'series_f',
                                            'series_g',
                                            'series_h',
                                            'series_j')) aq
               left join
/* Check if the target had an IPO, if it had IPO before the MA deal, then delete it.
We are looking only for private companies */
                 ( SELECT org_uuid AS ipo_org_uuid,
                          went_public_on
                  FROM `gcp-cset-projects.private_ai_investment.ipos_latest`) ipos
                 ON target_uuid = ipo_org_uuid
               WHERE went_public_on is Null
                 OR went_public_on < announced_on )target
            left join
              (SELECT * except(investor_uuidm)
               FROM
                   (SELECT * except(funding_round_uuid1)
                  FROM
/* get investor information */
                      (SELECT investor_name,
                              investor_uuid,
                              funding_round_uuid AS funding_round_uuid1
                     FROM private_ai_investment.investments_latest) inv
                  LEFT JOIN
/* get the number of investors */
                    (SELECT count(funding_round_uuid) AS inv_count,
                            funding_round_uuid,
                     FROM private_ai_investment.investments_latest
                     GROUP BY funding_round_uuid) inv_count
                    ON inv_count.funding_round_uuid = inv.funding_round_uuid1) Investors
               left join
                 (SELECT *
                  FROM
                      (SELECT *
                     FROM
/* Get the investor country */
                         (SELECT uuid AS investor_uuidm,
                                 country_code AS investor_country,
                                 region AS investor_province
                        FROM private_ai_investment.investors_latest) inv
                     LEFT JOIN
/* investor parent information */
                       (SELECT parent_uuid,
                               uuid AS child_uuid
                        FROM private_ai_investment.org_parents_latest) parent
                       ON inv.investor_uuidm = parent.child_uuid) Invest1
                  LEFT JOIN
/* Use parent country if avialable */
                    (SELECT uuid AS par_uuid1,
                            country_code AS parent_country,
                            name AS inv_parent_name
                     FROM private_ai_investment.organizations_latest) par_org
                    ON Invest1.parent_uuid = par_org.par_uuid1) InvestorInfo
                 ON Investors.investor_uuid = InvestorInfo.investor_uuidm) investors
              ON investors.funding_round_uuid = target.funding_round_uuid) funding
         LEFT JOIN
/* get PermID and Company ID to match with Refinitiv */
           (SELECT uuid AS OrgID,
                   cb_url AS Crunchbase_ID_target,
                   region AS target_province,
            FROM private_ai_investment.organizations_latest) id
           ON funding.target_uuid = id.OrgID) crunch
      LEFT JOIN
        (SELECT distinct CAST(PermID AS string) PermID_cr,
                         Crunchbase_ID,
                         COMPANY_ID
         FROM `gcp-cset-projects.private_ai_investment.CB_Ref_match_latest`
         WHERE COMPANY_ID is not NULL
           AND Crunchbase_ID is not NULL) CR_merge
        ON crunch.Crunchbase_ID_target = CR_merge.Crunchbase_ID) CB /* add company country */
   LEFT JOIN
/* Assign country from Refinitiv if availabe as more reliable */
     (SELECT COMPANY_ID AS COMPANY_ID1,
             NATION AS COUNTRY_Ref
      FROM `gcp-cset-projects.private_ai_investment.Target_latest`) Ref
     ON CB.COMPANY_ID = Ref.COMPANY_ID1)
/* Make sure the company is private */
WHERE target_uuid not in
         (SELECT org_uuid
          FROM `gcp-cset-projects.private_ai_investment.ipos_latest`)
       OR target_uuid is Null ORDER by round_id
       )