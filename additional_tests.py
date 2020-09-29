import pandas as pd
import numpy as np
from helpers.functions import table_func, agg_inv
import textwrap
import json
def add_acc_message(s):
    acc_message = '\n' +  '\n'.join(textwrap.wrap(s,120))
    print('\n'.join(textwrap.wrap(s,120)))
    with open('debug.txt', 'a') as file:
        file.write(acc_message)

print("Will run miscellaneous querries.")
def China_qrs(inv):
    # This code calculates miscellaneous tests that were not included in the main text:
    # Here were are looking at China specific Deals
    China_2016_3 = inv.loc[(inv['year'] == 2016) & (inv['application_code'] == 3) & (inv['Target_Region'] == 'China')]
    China_2017_3 = inv.loc[(inv['year'] == 2017) & (inv['application_code'] == 3) & (inv['Target_Region'] == 'China')]
    China_2017_17 = inv.loc[(inv['year'] == 2017) & (inv['application_code'] == 17) & (inv['Target_Region'] == 'China')]
    US_2014_17 = inv.loc[(inv['year'] == 2017) & (inv['application_code'] == 17) & (inv['Target_Region'] == 'China')]
    US_2014_all_MA = inv.loc[(inv['year'] == 2014) & (inv['MA']=='MA') & (inv['Target_Region'] == 'USA')]
    CH_2016_all_MA = inv.loc[(inv['year'] == 2016) & (inv['MA']=='MA') & (inv['Target_Region'] == 'China')]
    from_CH_to_US_all = inv.loc[(inv['year'] == 2016) &
                                (inv['China_inv']==1) & (inv['application_code'] == 10) & (inv['Target_Region'] == 'USA') ]
    China_buyer_MA = inv.loc[(inv['MA']=='MA') & (inv['China_inv']==1)]
    All_deals_8_2018 = inv.loc[(inv['year'] == 2018) & (inv['application_code'] == 8) ]
    US_all_MAs = inv.loc[(inv['MA']=='MA') & (inv['Target_Region'] == 'USA') ]
    ROW_target_2020 = inv.loc[(inv['year'] == 2020) &  (inv['Target_Region'] == 'ROW')]
    CHE_Target = inv.loc[inv['target_country'] == 'CHE']
    with pd.ExcelWriter('data/MA_test.xlsx') as writer:  # doctest: +SKIP
        China_2016_3.to_excel(writer, sheet_name='China_2016_3')
        China_2017_3.to_excel(writer, sheet_name='China_2017_3')
        China_2017_17.to_excel(writer, sheet_name='China_2017_17')
        US_2014_17.to_excel(writer, sheet_name='US_2014_17')
        US_2014_all_MA.to_excel(writer, sheet_name='US_2014_all_MA')
        CH_2016_all_MA.to_excel(writer, sheet_name='CH_2016_all_MA')
        from_CH_to_US_all.to_excel(writer, sheet_name='from_CH_to_US_all_MA')
        China_buyer_MA.to_excel(writer, sheet_name='China_buyer_MA')
        All_deals_8_2018.to_excel(writer, sheet_name='All_deals_8_2018')
        US_all_MAs.to_excel(writer, sheet_name='US_all_MAs')
        ROW_target_2020.to_excel(writer, sheet_name='ROW_target_2020')
        CHE_Target.to_excel(writer, sheet_name='CHE_Target')
    return

# this function reports statistics on the missing values.
def mis_inv(ref_ai, cb_ai, inv):
    add_acc_message(f'We dropped mis_ref becuase we could not match them to Refinitiv out of '
          f'{len(ref_ai["COMPANY_ID"].unique())} companies in Refinitiv. In Crunchbase there '
          f'{len(cb_ai["Crunchbase_ID"].unique())}')
    # Get statistics on the transactsion with missing transactions with investor countries
    # create missing investor country indicator
    inv.loc[inv['investor_country'].isna(),'inv_country_missing'] =1
    inv.loc[inv['investor_country'].notna(),'inv_country_missing'] =0
    # sum transactions by missing country
    table_miss = pd.pivot_table(inv.loc[inv['Target_Region']=='USA'],  values=['inv_country_missing', 'China_inv',
                 'investment_value'],index=['Crunchbase_ID_target'], aggfunc=np.sum)
    # Calculate the number of targets with Chinese investment. To do it we aggregte all tranasctions by company,
    # to see if there are some transactsions with some
    table_miss['China_dum']= table_miss['China_inv'].apply(lambda x: 1 if x>0 else 0)
    # same to calculate if there are companies with investments from investors with unknown countries
    table_miss['Miss_inv_country_dum']= table_miss['inv_country_missing'].apply(lambda x: 1 if x>0 else 0)
    # indicator if the firm received from both investors from unknown countries and investors from China
    table_miss.loc[(table_miss['Miss_inv_country_dum'] == 1) & (table_miss['China_dum']==1), 'Both'] = 1
    table_miss.loc[table_miss['Both'].isna(), 'Both'] = 0
    # indicator if the firm received from neighter investors from unknown countries and investors from China
    table_miss.loc[(table_miss['Miss_inv_country_dum'] == 0) & (table_miss['China_dum']==0), 'Neither'] = 1
    table_miss.loc[table_miss['Neither'].isna(), 'Neither'] = 0
    # add counter variables
    table_miss['all']= 1
    sum_miss = table_miss[['China_dum', 'Miss_inv_country_dum', 'Both', 'Neither','all']].sum()
    add_acc_message(f'US companies at least one transaction involving Chinese investors or Investors from missing countries'
                    f' {sum_miss}')
    # Export average values of Chinese investment in the US.
    # Save results for China/Unknown investors
    agg_inv('sum', table_miss, ['China_dum', 'Miss_inv_country_dum', 'all'], ['Both', 'Neither'], ['investment_value']).\
        to_csv('data/china_us_val.csv')
    av_us_vc = inv.loc[(inv['Target_Region']=='USA') & (inv['investment_value'].isna()) & (inv['MA'] =='VC'),
            'investment_value_median'].mean()
    av_ch_vc = inv.loc[(inv['Target_Region']=='China') & (inv['investment_value'].isna()) & (inv['MA'] =='VC') ,
                       'investment_value_median'].mean()
    av_us_vc_med = inv.loc[(inv['Target_Region']=='USA') & (inv['investment_value'].isna()) & (inv['MA'] =='MA'),
            'investment_value_median'].mean()
    av_ch_vc_med = inv.loc[(inv['Target_Region']=='China') & (inv['investment_value'].isna()) & (inv['MA'] =='MA') ,
                           'investment_value_median'].mean()
    add_acc_message(f'Averages deal sizes with imputed missing values: US VC deal {av_us_vc}, average China VC deal '
                    f'{av_ch_vc} average US MA deal {av_us_vc_med}, average China MA deal {av_ch_vc_med}')
    return

# calculate teh size of the average deal
def average_deal(inv):
    # Companies and rounds where there are investors with missing or Chinese investors
    comp_data = agg_inv('count',inv.loc[inv['Target_Region']=='USA'],['MA','China_inv'], ['inv_country_missing'], ['Crunchbase_ID_target'])
    round_data = agg_inv('count',inv.loc[inv['Target_Region']=='USA'],['MA','China_inv'], ['inv_country_missing'], ['round_id'])
    #save the results
    comp_data.to_csv('data/comp.csv')
    round_data.to_csv('data/round_data.csv')
    # Get the number of US deals
    us_inv_rnd = inv.loc[inv['Target_Region']=='USA', 'round_id'].nunique()
    print(f"Number of unique US deals {us_inv_rnd}")
    # Get the number of US companies that received investment
    c= inv.loc[inv['Target_Region']== 'USA','Crunchbase_ID_target'].nunique()
    print(f'Over the period {c} American companies received private equity investment')
    ch = inv.loc[(inv['Target_Region']== 'USA')& inv['China_inv']== 1,'Crunchbase_ID_target'].nunique()
    print(f'Over the period {ch} American companies received private equity investment  from China')
    undisc =  inv.loc[(inv['Target_Region']== 'USA')& inv['investor_country'].isna(),'Crunchbase_ID_target'].nunique()
    print(f'Over the period {undisc} American companies received private equity investment from at least one investor with'
          f'undisclosed country.')
    agg_inv('sum',inv[inv['Target_Region']== 'USA'].loc[inv['investment_value'].notna()], ['Target_Region', 'MA'], ['year'], ['investment_value'])
    # table

    return

# export additional tests:
def add_tests(inv):
    # sum investments by target and year
    sum_t = agg_inv('sum', inv.loc[inv['investment_value'].notna()], ['Target_Region', 'MA'], ['year'],
                    ['investment_value'])
    # count investments by target and year
    count_t = agg_inv('count', inv.loc[inv['investment_value'].notna()], ['Target_Region', 'MA'], ['year'],
                      ['round_id'])
    # replace zero count with 1 to avoid division  by zero
    count_t = count_t.replace(0, 1)
    # calculate average deal value
    av_deal_values = sum_t.values / count_t.values
    sum_t_av = sum_t
    sum_t_av[:] = av_deal_values
    # find missing deal vlaues
    inv.loc[inv['investment_value'].isna(), 'missing_inv_values'] = 1
    inv.loc[inv['investment_value'].notna(), 'missing_inv_values'] = 0
    # calcualte total number of missing deals with investment round as an observation.
    tot_miss = agg_inv('count', inv.loc[inv['investment_value'].isna()], ['Target_Region', 'MA'], ['year'],
                       ['round_id'])
    tot_non_miss = agg_inv('count', inv, ['Target_Region', 'MA'], ['year'], ['round_id'])
    # average number of missing values
    av_miss = tot_miss / tot_non_miss
    # report the number of missing values
    add_acc_message(f"Average share of missing investment values {av_miss}")
    # loop separtely at MA transactions:
    miss_ma_inv = inv.loc[(inv['missing_inv_values']==1) & (inv['MA']=='MA'), 'missing_inv_values'].sum()
    tot_ma = len(inv.loc[inv['MA']=='MA'])
    add_acc_message(f"Average share of missing investment values in MA transactsion is  {miss_ma_inv/tot_ma}, the number"
                    f" of missing is {miss_ma_inv} out of {tot_ma}")
    add_acc_message(
        f"We cover {inv['round_id'].count()} deals, out which {inv.loc[inv['investment_value'].isna(), 'round_id'].count()}"
        f" miss investment value")
    add_acc_message(f"We cover {inv.loc[inv['MA'] == 'VC', 'round_id'].nunique()} VC rounds, out which"
                    f" {inv.loc[(inv['investment_value'].isna()) & (inv['MA'] == 'VC'), 'round_id'].nunique()}"
                    f" miss investment value")
    add_acc_message(f"We cover {inv.loc[inv['MA'] == 'VC', 'round_id'].count()} VC rounds/Company combinations, out which"
                    f" {inv.loc[(inv['investment_value'].isna()) & (inv['MA'] == 'VC'), 'round_id'].count()}"
                    f" miss investment value")
    add_acc_message(f"We cover {inv.loc[inv['MA'] == 'MA', 'round_id'].count()} MA deals, out which"
                    f" {inv.loc[(inv['investment_value'].isna()) & (inv['MA'] == 'MA'), 'round_id'].count()}"
                    f" miss investment value")



    # Chinese->US flows by investment stage count
    count_stage_CH_US = agg_inv('count',inv.loc[(inv['MA']== 'VC') & (inv['Target_Region']=='USA')], ['investment_type',
                     'China_inv'], ['year'], ['round_id'])
    # Chinese->US flows by investment stage median value
    sum_stage_CH_US_med = agg_inv('sum',inv.loc[(inv['MA']== 'VC') & (inv['Target_Region']=='USA')],
                                  ['investment_type', 'China_inv' ], ['year'], ['investment_value_median'])
    # Chinese->US flows by investment stage disclosed value
    sum_stage_CH_US = agg_inv('sum', inv.loc[(inv['MA']== 'VC') & (inv['Target_Region']=='USA')],
                                  ['investment_type', 'China_inv' ], ['year'], ['investment_value'])
    # Undisclosed identity of the descination country indicators
    inv.loc[inv['investor_country'].isna(), 'inv_unknown'] = 1
    inv.loc[inv['investor_country'].notna(), 'inv_unknown'] = 0
    # Unknown->US flows by investment stage count
    count_stage_UKN_US = agg_inv('count',inv.loc[(inv['MA']== 'VC') & (inv['Target_Region']=='USA')], ['investment_type',
                     'inv_unknown'], ['year'], ['round_id'])
    # Unknown->US flows by investment stage median value
    sum_stage_UKN_US_med = agg_inv('sum', inv.loc[(inv['MA']== 'VC') & (inv['Target_Region']=='USA')],
                                  ['investment_type', 'inv_unknown' ], ['year'], ['investment_value_median'])
    # Unknown->US flows by investment stage disclosed value
    sum_stage_UKN_US = agg_inv('sum', inv.loc[(inv['MA']== 'VC') & (inv['Target_Region']=='USA')],
                                  ['investment_type', 'inv_unknown' ], ['year'], ['investment_value'])

    # Target country
    # Get more countries other than US, China and ROW
    inv = inv.replace('GBR', 'UK')
    inv = inv.replace('CA', 'CAN')
    inv = inv.replace('IS', 'ISR')
    inv = inv.replace('IN', 'IND')
    inv = inv.replace('DE', 'DEU')
    inv = inv.replace('SG', 'SGP')
    inv = inv.replace('JP', 'JPN')
    inv = inv.replace('FR', 'FRA')
    # Create data for the Next8 countries:
    others_countries =  inv.loc[inv['target_country'].isin(['UK', 'CAN', 'ISR','IND', 'SGP', 'JPN', 'FRA', 'DEU'])]
    count_other_majors = agg_inv('count',others_countries, ['target_country','MA'], ['year'], ['round_id'])
    # Unknown->US flows by investment stage median value
    other_majors_med = agg_inv('sum',others_countries, ['target_country','MA'], ['year'], ['investment_value_median'])
    # Unknown->US flows by investment stage disclosed value
    other_majors_val = agg_inv('sum',others_countries, ['target_country','MA'], ['year'], ['investment_value'])
    with pd.ExcelWriter('data/add_test.xlsx') as writer:  # doctest: +SKIP
        sum_t_av.to_excel(writer, sheet_name='Average_deal_value')
        av_miss.to_excel(writer, sheet_name='Share_of_missing_values')
        tot_miss.to_excel(writer, sheet_name='Total_missing_value_count')
        count_stage_CH_US.to_excel(writer, sheet_name='Count_CH_to_US')
        sum_stage_CH_US_med.to_excel(writer, sheet_name='MedVal_CH_to_US')
        sum_stage_CH_US.to_excel(writer, sheet_name='DiscVal_CH_to_US')
        count_stage_UKN_US.to_excel(writer, sheet_name='Count_UKN_to_US')
        sum_stage_UKN_US_med.to_excel(writer, sheet_name='MedVal_UKN_to_US')
        sum_stage_UKN_US.to_excel(writer, sheet_name='DiscVal_UKN_to_US')
        count_other_majors.to_excel(writer, sheet_name='Others_Count')
        other_majors_med.to_excel(writer, sheet_name='Others_Med')
        other_majors_val.to_excel(writer, sheet_name='Others_Val')
    return

# Calculate statistics of the CB Refinitiv match
def cb_ref_match_stat():
    # Read the list of merged transactions
    joint = pd.read_csv('data/joint_CB_Ref_transactions.csv')
    # matched to both
    c1 = joint.loc[(joint['Crunchbase_ID_target'].notna()) & (joint['Crunchbase_ID_m'].notna()), 'year'].count()
    # unmatched to Refinitiv
    c2 = joint.loc[(joint['Crunchbase_ID_target'].notna()) & (joint['Crunchbase_ID_m'].isna()), 'year'].count()
    # total firms in Crunchbase
    c3 = joint.loc[joint['Crunchbase_ID_target'].notna(), 'year'].count()
    # report results
    add_acc_message(f'When we matched Refinitiv and Cruncbase by year and companies ID. In Crunchbase we have {c3}'
                    f' firm/year observations,'
          f'\n out which we matched {c1} to Refinitiv and unmatched {c2}.')
    # Number of Crunchbase minus the number of Refinitiv in the joins transaction file
    cr_num = len(set(joint['Crunchbase_ID_target'].unique()) - set(joint['Crunchbase_ID_m'].unique()))
    # total number of companies in Crunchbase
    tot_cr = len(joint['Crunchbase_ID_target'].unique())
    # total number of companies in Refinitiv
    tot_ref = len(set(joint['Crunchbase_ID_m'].unique()))
    add_acc_message(f'In the jount transations file, there are {tot_cr} Crunchbase companies, and {tot_ref} '
        f'Refinitiv companies, the \n {cr_num} Crunchbase companies were matched with Refinitiv')
    # For the transactsions that are not exactly the same, check the number where the difference is less than $500K
    joint= joint.fillna(0)
    # calculate the absolute difference in teh transaction values:
    joint['diff_abs'] = abs(joint['ROUND_TOTAL_CB'] - joint['ROUND_TOTAL_REF'])
    joint['diff'] = joint['ROUND_TOTAL_CB'] - joint['ROUND_TOTAL_REF']
    med= joint['diff'].median()
    mean = joint['diff'].mean()
    add_acc_message(f'On average Crunchbase transation is larger than Refinitiv transaction by {mean}'
                    f', the median difference is {med}')

    # indicator of no-different
    no_diff = joint.loc[joint['diff_abs'] == 0, 'year'].count()
    # indicator of difference less than $500,000
    small_diff =  joint.loc[(joint['diff_abs'] > 0) & (joint['diff_abs'] <= 0.5), 'year'].count()
    # indicator of different of more than
    large_diff = joint.loc[joint['diff_abs'] > 0.5, 'year'].count()
    add_acc_message(f'Amonng matched company/year pairs in Refinitiv and Crunchbase.  {no_diff} have no difference in total'
        f' transactions \n{small_diff} have difference of less than $500,000 , and {large_diff} has a difference more than'
                    f' $500,000 .')
    #For the transactsion where the difference was larger than $500, check whether CB or Ref was higher
    c_over_r = joint.loc[(joint['ROUND_TOTAL_CB']>joint['ROUND_TOTAL_REF'].notna()) &
                         (joint['Crunchbase_ID_target'].notna())& (joint['Crunchbase_ID_m'].notna()) &
                         (joint['diff_abs'] > 0.5),'year'].count()

    r_over_c = joint.loc[(joint['ROUND_TOTAL_CB']<joint['ROUND_TOTAL_REF'].notna())&(joint['Crunchbase_ID_target'].notna()
                         & (joint['diff_abs'] > 0.5)) & (joint['Crunchbase_ID_m'].notna()),'year'].count()
    add_acc_message(f'For {c_over_r} company/year pairs the crunchbase total was higher, for {r_over_c} the Refinitiv'
                    f' total was higher ')
    return



def mask(inv):
    # create dict with fake IDs
    def dic_mask_id(df, cols):
        for c in range(0,len(cols)):
            if c == 0:
                set_ids = set(df[cols[c]].unique())
            else:
                set_ids = set_ids.union(set(df[cols[c]].unique()))
        dic = {}
        ar = list(set_ids)
        for i in range(0,len(ar)):
            dic[ar[i]] = i
        return dic
    # create fake IDs
    dic_CB_url = dic_mask_id(inv, ['Crunchbase_ID_target','Crunchbase_ID' ])
    dic_uuid = dic_mask_id(inv, ['investor_uuid','target_uuid' ])
    dic_name = dic_mask_id(inv, ['investor_name','target_name' ])
    dic_round = dic_mask_id(inv, ['round_id'])
    # replace IDs with fake IDs
    for d in [dic_CB_url, dic_uuid,dic_name, dic_round]:
        if d == dic_CB_url:
            inv['Crunchbase_ID_target'] = inv['Crunchbase_ID_target'].apply(lambda x: d[x])
            inv['Crunchbase_ID'] = inv['Crunchbase_ID'].apply(lambda x: d[x])
        if d == dic_uuid:
            inv['investor_uuid'] = inv['investor_uuid'].apply(lambda x: d[x])
            inv['target_uuid'] = inv['target_uuid'].apply(lambda x: d[x])
        if d == dic_name:
            inv['investor_name'] = inv.loc[inv['investor_name'].notna(), 'investor_name'].apply(lambda x: d[x])
            inv['target_name'] = inv['target_name'].apply(lambda x: d[x])
        if d == dic_round:
            inv['round_id'] = inv['round_id'].apply(lambda x: d[x])
    # save masked data
    inv.to_csv('data/masked_inv.csv')
    # save dictionaries
    with open('data/dic_CB_url.json', 'w') as fp:
        json.dump(dic_CB_url, fp, sort_keys=True)
    with open('data/dic_uuid.json', 'w') as fp:
        json.dump(dic_uuid, fp, sort_keys=True)
    with open('data/dic_round.json', 'w') as fp:
        json.dump(dic_round, fp, sort_keys=True)
    return



