# clean data and load data
import os
import pandas as pd
from .gcs_storage import list_blobs, delete_blob, download_blob, upload_blob, BQ_to_bucket
import textwrap
def add_acc_message(s):
    # This function create
    acc_message = '\n' +  '\n'.join(textwrap.wrap(s,120))
    print('\n'.join(textwrap.wrap(s,120)))
    with open('debug.txt', 'a') as file:
        file.write(acc_message)

# function to create aggegation tables. It bases the type of aggregation on the table name: func parameter
def agg_inv(func, data, factors, columns, stat):
    all_agg = factors + columns + stat
    fac_col = factors + columns
    if func == 'mean':
        data = data[all_agg].groupby(fac_col).mean().unstack(fill_value=0).stack().reset_index(fac_col)
    elif func == 'sum':
        data = data[all_agg].groupby(fac_col).sum().unstack(fill_value=0).stack().reset_index(fac_col)
    elif func == 'count':
        data = data[all_agg].drop_duplicates().groupby(fac_col).count().unstack(fill_value=0).stack().\
            reset_index(fac_col)
    else:
        print("error in aggregation function")
    agg_table = pd.pivot_table(data, index=factors, columns=columns,values=stat)
    return agg_table

def table_func(main_tab_list, inv):
    dict = {}
    for n in main_tab_list:
        factors = ['Target_Region']
        # find out the function
        if 'total' in n:
            func = 'sum'
        if 'aver' in n:
            func = 'mean'
        elif 'count' in n:
            func = 'count'
        # find out pivot variable
        if 'count' in n:
            stat = ['round_id']
        elif 'med' in n:
            stat = ['investment_value_median']
        else:
            stat = ['investment_value']
        # categories for aggregation
        if 'China' in n:
            factors.append('China_inv')
        if 'MA' in n:
            factors.append('MA')
        if 'app' in n:
            factors.append('application_code')
        if 'sec' in n:
            factors.append('security')
        dict[n] = agg_inv(func, inv, factors, ['year'], stat)
    return dict

















# test for the same export tables that are likely errors
def dict_test(dic):
    for k1 in dic:
        for k2 in dic:
            if (k1 != k2) & (dic[k1].equals(dic[k2])):
                print(f"ERROR: Same table values {k1} and {k2}.")

# removes files in the directory
def remove_dir(name):
    last_char_index = name.rfind("/") + 1
    save_file_name = name[last_char_index:]
    return save_file_name

# Load and clean data
def load_clean():
    cb_ref_match = pd.read_csv('data/matched_companies.csv')
    # MA deals in Crunchabse for all companies
    cb_ma = pd.read_csv('data/cb_ma.csv')
    # VC deals for all companies in Crunchbase
    cb_vc = pd.read_csv('data/cb_vc.csv')
    # VC deals in Refinitiv
    ref_vc = pd.read_csv('data/ref_vc.csv')
    # AI companies in Crunchbase
    cb_ai = pd.read_csv('data/ai_companies_crunchbase.csv')
    #AI companies in Refinitiv
    ref_ai = pd.read_csv('data/ai_companies_in_refinitiv.csv')
    # MA deals in Refinitiv
    ref_ma = pd.read_csv('Data/ref_ma.csv')
    add_acc_message(f"From BQ we read. Length of cb_ma: "
        f"{len(cb_ma)} , length of cb_vc:{len(cb_ma)}, length of cb_ai: {len(cb_ai)}")
    # Vadliation
    print('cleaning transactions')
    # Clean Ref VC from MA transactions
    ref_vc = ref_vc.rename(columns={'Year': 'year'})
    ref_ma = ref_ma.rename(columns={'year': 'year_ma'})
    # remove VC if there was a MA before:
    Ref_MA_dates = ref_ma
    # Merge MA and VC deals for Refinitiv. If the permID is missing, create fake permID to avoid matching on null values.
    # So the observations will not be matched on fake IDs.
    Ref_MA_dates.loc[Ref_MA_dates['TMTOrganizationPermID'].isna(), 'TMTOrganizationPermID'] = -99
    ref_vc.loc[ref_vc['TMTOrganizationPermID'].isna(), 'TMTOrganizationPermID'] = -100
    # MA deals if the investor is a public company
    Ref_MA_dates = Ref_MA_dates[['TMTOrganizationPermID','year_ma']].drop_duplicates()
    ref_vc = ref_vc.drop_duplicates()
    ref_vc = ref_vc.merge(Ref_MA_dates, on = 'TMTOrganizationPermID', how='left')
    ref_vc  = ref_vc.drop_duplicates()
    # get dates for MA deals
    CB_MA_dates = cb_ma[['target_uuid', 'year']].rename(columns={'year': 'ma_year'})
    len(CB_MA_dates)
    # the target had sevaral MA deals we keep the eariers.
    CB_MA_dates = CB_MA_dates.sort_values(by=['target_uuid', 'ma_year']). \
        drop_duplicates('target_uuid', keep='first')
    cb_vc = cb_vc.merge(CB_MA_dates, how='left', on='target_uuid')
    mis_ref = len(ref_ai.loc[ref_ai['Crunchbase_ID'].isna(), 'COMPANY_ID'].unique())
    add_acc_message(f' We matched Crunchbase to the AI companies in Refinitiv.'
        f'We were unable to match {mis_ref} Refintiv companies to Crunchbase.'
        f'Out of {len(ref_ai["COMPANY_ID"].unique())}, companies in Refinitiv  we matched'
        f'{len(cb_ai["Crunchbase_ID"].unique())} to Crunchbase.')
    # Concert USD to millions
    cb_vc['investment_value'] = cb_vc['investment_value'] / 1000000
    cb_vc['target_valuation'] = cb_vc['target_valuation'] / 1000000
    cb_vc['ROUND_TOTAL'] = cb_vc['ROUND_TOTAL'] / 1000000
    cb_ma['investment_value'] = cb_ma['investment_value'] / 1000000
    cb_ma['target_valuation'] = cb_ma['target_valuation'] / 1000000
    return cb_ma, cb_vc, ref_vc, cb_ai, ref_ai, ref_ma, cb_ref_match


# function convert countries to three regions US, China (+HK) and rest of the world (ROW).
# This function will be called by the clean_country function.
def country_to_region(df):    
    df['Target_Region'] = None
    df.loc[df['target_country'] == 'USA', 'Target_Region'] = 'USA'
    df.loc[(df['target_country'] == 'CHN') | (df['target_country'] == 'HKG'), 'Target_Region'] = 'China'
    df.loc[(df['Target_Region'].isna()) & (df['target_country'].notna()), 'Target_Region'] = 'ROW'
    df.loc[df['Target_Region'].isna(), 'Target_Region'] = 'Unknown'
    df['investor_Region'] = None
    df.loc[(df['investor_country'] == 'CHN') | (df['investor_country'] == 'HKG'), 'investor_Region'] = 'China'
    df.loc[df['investor_country'] == 'USA', 'investor_Region'] = 'USA'
    df.loc[(df['investor_Region'].isna()) & (df['investor_country'].notna()), 'investor_Region'] = 'ROW'
    # drop missing Nation's data (often duplicate records)
    df.loc[(df['investor_Region'].isna()) | (df['investor_country'] == 'UN'), 'investor_Region'] = 'Unknown'
    return df

# Clean country affiliation.
def clean_country(cb_vc, cb_ma, ref_vc):
    # REPLACE COUNTRY IN CB BY REFINITIV COUNTRY if available because Refinitiv has better quality country data
    # Convert country codes from 2 to 3 letters
    cb_vc['COUNTRY_Ref'] = cb_vc['COUNTRY_Ref'].replace({'CH': 'CHN', 'US': 'USA', 'HK': 'HKG'})
    ref_vc['investor_country'] = ref_vc['investor_country'].replace({'CH': 'CHN', 'US': 'USA', 'HK': 'HKG'})
    ref_vc['target_country'] = ref_vc['target_country'].replace({'CH': 'CHN', 'US': 'USA', 'HK': 'HKG'})
    # add MA country check.
    # replace VC country with Refinitiv country if available
    cb_vc.loc[cb_vc['COUNTRY_Ref'].notna(), 'target_country'] = \
        cb_vc.loc[cb_vc['COUNTRY_Ref'].notna(), 'COUNTRY_Ref']
    cb_ma['COUNTRY_Ref'] = cb_ma['COUNTRY_Ref'].replace({'CH': 'CHN', 'US': 'USA', 'HK': 'HKG'})
    cb_ma.loc[cb_ma['COUNTRY_Ref'].notna(), 'target_country'] = \
        cb_ma.loc[cb_ma['COUNTRY_Ref'].notna(), 'COUNTRY_Ref']
    # replace with ultimate investor in VC and MA for name and country where available
    cb_ma.loc[cb_ma['inv_parent_name'].notna(), 'investor_name'] = cb_ma.loc[cb_ma['inv_parent_name'].
                                                                                             notna(), 'inv_parent_name']
    cb_ma.loc[cb_ma['parent_country'].notna(), 'investor_country'] = cb_ma.loc[cb_ma['parent_country'].
                                                                                               notna(), 'parent_country']
    cb_vc.loc[cb_vc['inv_parent_name'].notna(), 'investor_name'] = cb_vc.loc[cb_vc['inv_parent_name'].
                                                                                             notna(), 'inv_parent_name']
    cb_vc.loc[cb_vc['parent_country'].notna(), 'investor_country'] = cb_vc.loc[cb_vc['parent_country'].
                                                                                               notna(), 'parent_country']
    # Create stat on China's involvement at the transactional level.
    cb_vc.loc[(cb_vc['investor_country'] == 'CHN') | (cb_vc['investor_country'] == 'HKG'), 'China_inv'] = 1
    # for the rest of the transactions the China_inv =0
    cb_vc.loc[cb_vc['China_inv'].isna(), 'China_inv'] = 0
    # Asssign China_inv = 1 to all the whole transaction where it was involved in a round
    China_inv = cb_vc[['round_id', 'China_inv']].drop_duplicates().groupby(['round_id']).max()
    # delete old China_inv column.
    cb_vc = cb_vc.drop(columns='China_inv')
    # Match back China_inv to the data. Now the whole round will have the same China_involvement indicator.
    cb_vc = cb_vc.merge(China_inv, on=['round_id'], how='left')
    # same in MA, don't need to average, because there is only one investor per transaction
    cb_ma.loc[(cb_ma['investor_country'] == 'CHN') | (cb_ma['investor_country'] == 'HKG'), 'China_inv'] = 1
    cb_ma.loc[cb_ma['China_inv'].isna(), 'China_inv'] = 0
    # check results by showing top countries:
    add_acc_message(f'Top 10 target countries by count in ref_vc {ref_vc["target_country"].value_counts()[0:10]}')
    add_acc_message(f'Top 10 target countries by count in ref_vc {cb_vc["target_country"].value_counts()[0:10]}')
    add_acc_message(f'Top 10 target countries by count in ref_vc {ref_vc["target_country"].value_counts()[0:10]}')
    #Aggreate counties into larger regions:
    for c in [cb_vc, cb_ma, ref_vc]:
        country_to_region(c)
    return cb_vc, cb_ma, ref_vc

# Match companies extracted from the regular expression querry with 2 lists of AI companies curated by experts.
def validation_match(cb_ai, ref_ai):
    # load validation sets
    val_v1 = pd.read_csv('data/validation_set_v1.csv')
    val_v2 = pd.read_csv('data/validation_set_v2.csv')
    # number of companies matched to the validation sets in Refinitiv or Crunchbases:
    matched_v1 = val_v1.loc[(val_v1['crunchbase_url'].isin(ref_ai['Crunchbase_ID'].values)) | (val_v1['crunchbase_url']. \
        isin(cb_ai['Crunchbase_ID'].values)), 'crunchbase_url'].count()
    matched_v2 = val_v2.loc[(val_v2['Crunchbase URL'].isin(ref_ai['Crunchbase_ID'].values)) | (val_v2['Crunchbase URL']. \
        isin(cb_ai['Crunchbase_ID'].values)), 'Crunchbase URL'].count()
    # Report the match rate with validation list
    add_acc_message('From the v0 of the validation set of AI companies we matched '
        f'{matched_v1}, out of {len(val_v1)})')
    add_acc_message(f'From the v1 of the validation set of AI companies we matched ,' 
        f'{matched_v2} out of , {len(val_v2)}')

# Calculate the overlap between Crunchbase and Refinitiv
def overlap(ref_ai, cb_vc, cb_ai, ref_vc, ref_ma, cb_ref_match):
    print("Calculate Overlap between Refinitiv and Crunchbase transactions aggregatd by company and year.")
    add_acc_message(f'Number of Refinitiv companie we could not match to CB ,'
                                   f'{cb_ai["Crunchbase_ID"].isna().sum()}')
    # Merge CB and Ref in the overlap. Create unique round ID for Refinitiv data in one variable.
    ref_vc['round_id'] = ref_vc['ROUND_TOTAL'].astype(str) + ref_vc['COMPANY_ID'].astype(str) + ref_vc[
            'year'].astype(str)
    ref_ma['round_id'] = ref_ma['MASTER_DEAL_NO']
    # drop companies from CB that were unmatched to Ref in the IDs
    ref_ai = ref_ai[ref_ai['COMPANY_ID'].notna()]
    add_acc_message(f'Number of companies in Refinitiv {ref_ai["COMPANY_ID"].nunique()},'
        f' The number with missing CrunchbaseID {ref_ai["Crunchbase_ID"].isna().sum()}')
    add_acc_message(f'Number of companies in Crunchbase '
        f'{cb_ai["Crunchbase_ID"].nunique()} The number with missing COMPANY_ID,'
                                   f'{cb_ai["COMPANY_ID"].isna().sum()}')
    # Select variables for common transactions table
    VC_CB = cb_vc[['year', 'target_country', 'target_name', 'Target_Region', 'investor_Region', 'investment_value',
                       'target_valuation', 'investor_name', 'investor_country', 'Crunchbase_ID_target', 'COMPANY_ID',
                       'ROUND_TOTAL', 'round_id']]
    MA_CB = cb_vc[['year', 'target_country', 'target_name', 'Target_Region', 'investor_Region', 'investment_value',
                       'target_valuation', 'investor_name', 'investor_country', 'Crunchbase_ID_target', 'COMPANY_ID',
                       'ROUND_TOTAL', 'round_id']]
    # append data for aggregate statistics
    CB = VC_CB.append(MA_CB)
    # rename varibles to make the common schema
    ref_vc = ref_vc.rename(columns={'Year': 'year', 'FIRM_NAME': 'investor_name'})
    VC_R = ref_vc[['year', 'target_country', 'target_name', 'investment_value', 'investor_name', 'ROUND_TOTAL',
                       'OrganizationID', 'round_id', 'COMPANY_ID', 'Crunchbase_ID_m']]
    MA_R = ref_ma.merge(cb_ref_match,how='inner', left_on='OrganizationID', right_on='PermID_Ref')
    MA_R = MA_R.loc[:,['year_ma','VALUE','COUNTRY_Ref','Crunchbase_ID_CB', 'OrganizationID',
           'Name_Ref','PermID_Ref', 'round_id']].rename(columns={'year_ma':'year','VALUE':'investment_value',
           'COUNTRY_Ref':'target_country','Crunchbase_ID_CB':'Crunchbase_ID_m'})
    MA_R['ROUND_TOTAL'] = MA_R['investment_value']
    # Merge MA and VC in
    # Baselins Refinitiv is in the thausands, converted to the millions
    # restrict to the same companies and years
    VC_CB_AI_M = CB.loc[
        (CB['Crunchbase_ID_target'].isin(ref_ai['Crunchbase_ID'].values)) & (CB['year'] > 2012)]
    VC_Ref_AI_M = VC_R.loc[(VC_R['COMPANY_ID'].isin(ref_ai.loc[ref_ai['COMPANY_ID'].notna(), 'COMPANY_ID'].values)) & (
                VC_R['year'] > 2012)]
    VC_Ref_AI_M = VC_Ref_AI_M.append(MA_R)
    # Convert Refinitiv from thaisands to millions
    VC_Ref_AI_M['investment_value'] = VC_Ref_AI_M['investment_value'] / 1000
    VC_Ref_AI_M['ROUND_TOTAL'] = VC_Ref_AI_M['ROUND_TOTAL'] / 1000
    # set CB and R indicators
    VC_CB_AI_M.loc[:,'CB'] = 1
    VC_Ref_AI_M.loc[:,'CB'] = 0
    # merge CB and Ref data
    inv = VC_CB_AI_M.append(VC_Ref_AI_M, sort=True)
    ref_inv = len(inv['COMPANY_ID'].unique())
    add_acc_message(f' We found CrunchbaseID  for {ref_inv}  Refinitiv AI VC companies. '
        f'Consider overlop between CB and Ref using CrunchbaseID.')
    print(inv['investor_Region'].value_counts())
    print(inv['Target_Region'].value_counts())
    # Overlap for validation companies
    # drop duplicates
    inv = inv.drop_duplicates()
    inv['count'] = 1
    # save transactions
    inv.to_csv('data/overlap_transactions.csv')
    # fill in missing Crunchbased IDs to have a unique target DI
    inv.loc[inv['Crunchbase_ID_m'].isna(), 'Crunchbase_ID_m'] = inv.loc[inv['Crunchbase_ID_m'].isna(), 'Crunchbase_ID_target']
    inv.loc[inv['Crunchbase_ID_m'].isna(), 'Crunchbase_ID_m'] = inv.loc[inv['Crunchbase_ID_m'].isna(), 'OrganizationID']
    # calculate aggregate investment statistics by year
    over_tot = agg_inv('sum',inv, ['Target_Region', 'CB'], ['year'], ['investment_value'])
    over_ct = agg_inv('count',inv, ['Target_Region', 'CB'], ['year'], ['Crunchbase_ID_m'])
    over_tot_geo = agg_inv('sum',inv, ['Target_Region',  'CB'], ['year'], ['investment_value'])
    over_ct_geo = agg_inv('count', inv, ['Target_Region',  'CB'], ['year'], ['round_id'])
    with pd.ExcelWriter('data/CB_Ref_Overlap_stat.xlsx') as writer:  # Save aggregate overlap transactions
        over_tot.to_excel(writer, sheet_name='over_tot')
        over_ct.to_excel(writer, sheet_name='over_ct')
        over_tot_geo.to_excel(writer, sheet_name='over_tot_geo')
        over_ct_geo.to_excel(writer, sheet_name='over_ct_geo')
    # fill in missing transaction values with zero
    VC_CB_AI_M['ROUND_TOTAL'] = VC_CB_AI_M['ROUND_TOTAL'].fillna(0)
    VC_Ref_AI_M['ROUND_TOTAL'] = VC_Ref_AI_M['ROUND_TOTAL'].fillna(0)
    # aggregate data by year
    CB_agg = VC_CB_AI_M[['ROUND_TOTAL', 'year', 'COMPANY_ID', 'Crunchbase_ID_target']]. \
        drop_duplicates().groupby(['year', 'COMPANY_ID', 'Crunchbase_ID_target']). \
        sum().unstack().stack().reset_index(['year', 'COMPANY_ID', 'Crunchbase_ID_target']).drop_duplicates()
    Ref_agg = VC_Ref_AI_M[['ROUND_TOTAL', 'year', 'COMPANY_ID', 'Crunchbase_ID_m']]. \
        drop_duplicates().groupby(['year', 'COMPANY_ID', 'Crunchbase_ID_m']). \
        sum().unstack().stack().reset_index(['year', 'COMPANY_ID', 'Crunchbase_ID_m']).drop_duplicates()
    # create join transaction list by target and year
    joint_trans = CB_agg.merge(Ref_agg, left_on=['Crunchbase_ID_target', 'year'], right_on=['Crunchbase_ID_m', 'year']
                               , suffixes=('_CB', '_REF'), how='outer').drop_duplicates()
    # keep transactions where values are greater than zero.
    joint_trans = joint_trans[(joint_trans['ROUND_TOTAL_CB'] > 0) | (joint_trans['ROUND_TOTAL_REF'] > 0)]
    joint_trans = joint_trans.rename({'Crunchbase_ID': 'Crunchbase_ID_Ref', 'Crunchbase_ID_target': 'Crunchbase_ID_CB'})
    # export data
    joint_trans.to_csv('data/joint_CB_Ref_transactions.csv')

def impute_med(net, cb_vc, cb_ma):
    cb_ma.rename(columns={'investment_value': 'ROUND_TOTAL'})
    for f in [cb_vc, cb_ma]:
        # net is the list of variables used in the imputation
        # MA transactions don't have investment type information so if the investment type in the net list then
        # skip the MA transactions.
        if (f['MA'].iloc[1] != 'MA') | ('investment_type' not in net):
            # pandas aggregations work slightly differently for 1 or >1 columns.
            if len(net) > 1:
                Med = f[['ROUND_TOTAL'] + net].drop_duplicates().groupby(net).median().unstack().stack(). \
                    reset_index(net).drop_duplicates()
            else:
                Med = f[['ROUND_TOTAL'] + net].drop_duplicates().groupby(
                    net).median().unstack().to_frame(). \
                    stack().reset_index(net).drop_duplicates().rename(columns={0: 'ROUND_TOTAL'})
            # if the Median already exists in the data we need to replace the values rather than creating new columns
            if 'ROUND_TOTAL_Median' in f.columns:
                Med = Med.rename(columns={'ROUND_TOTAL': 'ROUND_TOTAL_Median_new'})
                f = f.merge(Med, how='left', on=net)
                f.loc[f['ROUND_TOTAL_Median'].isna(), 'ROUND_TOTAL_Median'] = f.loc[f['ROUND_TOTAL_Median'].isna(),
                                                                                    'ROUND_TOTAL_Median_new']
                f = f.drop(columns={'ROUND_TOTAL_Median_new'})
                # If there is not column 'ROUND_TOTAL_Median' we need to create it.
            else:
                Med = Med.rename(columns={'ROUND_TOTAL': 'ROUND_TOTAL_Median'})
                f = f.merge(Med, how='left', on=net)
        # If imputation is for VC save results in cb_vc, for MA save results in cb_ma
        if f['MA'].iloc[1] == 'VC':
            cb_vc = f
        if f['MA'].iloc[1] == 'MA':
            cb_ma = f
    return cb_vc, cb_ma

def impute_missing_values(cb_vc, cb_ma):
    '''
    Is this the code corresponding to "To produce the estimated figures in Section 3, we used a multistage estimation process..." ?
    :param cb_vc: list of VC transactions of Crunchbase
    :param cb_ma: list of MA transactions for Crunchbase
    :return: cb_vc and cb_ma with imputed missing values
    '''
    cb_ma['MA'] = 'MA'
    cb_vc['MA'] = 'VC'
    cb_ma = cb_ma.rename(columns={'investment_value': 'ROUND_TOTAL'})
    # set imputation levels
    imputation_net = [['year', 'target_country', 'investment_type'], ['year', 'target_country'], ['year', 'investment_type'],
    ['target_country', 'investment_type'], ['year']]
    # run imputations. net is the combination of variables used in the imputation. The more variables the finer the
    # imputation.
    for net in imputation_net:
        print(net)
        cb_vc, cb_ma = impute_med(net,cb_vc,cb_ma)
    # Substitute missing values with medians
    cb_ma = cb_ma.rename(columns={'ROUND_TOTAL': 'investment_value' , 'ROUND_TOTAL_Median': 'investment_value_median'})
    cb_ma.loc[cb_ma['investment_value'].notna(), 'investment_value_median'] = \
        cb_ma.loc[cb_ma['investment_value'].notna(), 'investment_value']
    cb_vc['investment_value_median'] = cb_vc['investment_value']
    cb_vc.loc[cb_vc['investment_value_median'].isna(), 'investment_value_median'] = \
        cb_vc.loc[cb_vc['investment_value_median'].isna(), 'ROUND_TOTAL_Median'] / \
        cb_vc.loc[cb_vc['investment_value_median'].isna(), 'inv_count']
    add_acc_message(f'We median imputation has {cb_vc["investment_value_median"].isna().sum()}'
          f' missing obs. Before imputation we had {cb_vc["investment_value"].isna().sum()} missing obs.')
    return cb_ma, cb_vc

def clean_VC_after_MA(cb_vc, cb_ma, ref_vc):
    # drop VC transactsions after MA transaction.
    rvc_len = len(ref_vc)
    cvc_len = len(cb_vc)
    ref_vc = ref_vc.loc[(ref_vc['year_ma'] > ref_vc['year']) | (ref_vc['year_ma'].isna())]
    ref_vc = ref_vc.drop(columns={'year_ma'})
    ref_vc = ref_vc.drop_duplicates()
    # delete VC transactions if the there was MA before it
    cb_vc = cb_vc.loc[(cb_vc['year'] <= cb_vc['ma_year']) | cb_vc['ma_year'].isna()]
    add_acc_message(f'Number of transactions with missing Round total '
                    f'{len(cb_vc.loc[cb_vc["ROUND_TOTAL"].isnull()])}')
    add_acc_message(f'Number of transactions with missing MA total '
                    f' {len(cb_ma.loc[cb_ma["investment_value"].isnull()])}')
    # Substitue round_id with target_uuid + year for MA
    cb_ma.loc[:,'round_id'] = cb_ma['target_uuid'] + cb_ma['year'].astype(str)
    add_acc_message(f"Dropped {cvc_len - len(cb_vc)} transactions from Crunchbase, because"
        f"there was an MA prior to it.  \n Dropped {rvc_len - len(ref_vc)} transactions from Refinitiv, because"
        f"there was an MA prior to it. ")
    return (cb_vc, cb_ma, ref_vc)

def joint_inv_st(inv, ref_ai, cb_vc, cb_ma, vc_ai_ncomp_ID, ma_ai_ncomp_ID, cb_ai):
    '''
     This function generates the unified set of AI companies used as the basis for the report.
     :param inv: list of merged
     :param ref_ai: AI companies in refinitiv
     :param cb_vc: Crunchbase VC transactions
     :param cb_ma: Crunchbase MA transactions
     :param vc_ai_ncomp_ID: list of unique IDs in Crunchbase VC
     :param ma_ai_ncomp_ID: list of unique IDs in Crunchbase MA
     :param cb_ai: AI companies identified in Crunchbase
     :return: None
     '''
    add_acc_message(f'Number companies with missing application code , '
                    f'{inv.loc[inv["application_code"].isna(),"Crunchbase_ID_target"].nunique()}')
    if inv['application_code'].isna().sum() > 0:
        miss_app_export = cb_ai.loc[cb_ai['Crunchbase_ID'].isin(inv.loc[inv["application_code"].isna(),
        "Crunchbase_ID_target"].to_list()), ['Crunchbase_ID', 'description', 'description_ref']].merge(ref_ai.
        loc[ref_ai['Crunchbase_ID'].isin(inv.loc[inv["application_code"].isna(), "Crunchbase_ID_target"].to_list()),
        ['Crunchbase_ID','description_ref']],on='Crunchbase_ID',how='outer').drop_duplicates()
        miss_app_export['description_ref'] = miss_app_export['description_ref_y']
        miss_app_export.loc[miss_app_export['description_ref'].isna(), 'description_ref'] = \
            miss_app_export.loc[miss_app_export['description_ref'].isna(), 'description_ref_x']
        miss_app_export = miss_app_export.drop(columns=['description_ref_x', 'description_ref_y'])
        add_acc_message("Exporting missing applications companies to fill the data gaps to"
                                                  "debug/missing_app_codes.csv")
        miss_app_export.to_csv('debug/missing_app_codes.csv')
    add_acc_message(f'Check missing data, missing investor country: '
        f'{inv["investor_country"].isna().sum()}, missing target country: {inv["target_country"].isna().sum()},'
                    f' missing China investor {inv["China_inv"].isna().sum()}')
    add_acc_message(f'Number of Refinitiv companies in the database ,'
        f'{len(inv.loc[inv["Crunchbase_ID_target"].isin(ref_ai["Crunchbase_ID"].unique()), "Crunchbase_ID_target"].unique())}')
    add_acc_message(f'Number of strictly CB companies in the database,'
        f' {len(inv.loc[~inv["Crunchbase_ID_target"].isin(ref_ai["Crunchbase_ID"].unique()), "Crunchbase_ID_target"].unique())}')
    add_acc_message(f'Number of companies in the final data: ,'
                                  f'{len(inv["Crunchbase_ID_target"].unique())}')
    # don't worry about it
    add_acc_message(f'In Crunchbase we identified earlier,'
          f' {cb_vc["Crunchbase_ID_target"].append(cb_ma["Crunchbase_ID_target"]).nunique()},  AI companies. '
          f' {vc_ai_ncomp_ID} companies received VC funding.,'
          f' {ma_ai_ncomp_ID}  companies were targets of MA transacitons. The same company can both receive VC funding and'
                          ' be a target in MA tranaction after 2013.')
    a = set(ref_ai['Crunchbase_ID'].unique())
    b = set(cb_ai['Crunchbase_ID'].unique())
    inv.loc[inv['MA'] == 'VC', ['round_id', 'investment_value']].drop_duplicates().to_csv('data/test.csv')
    add_acc_message(f'There are in total  {len(a.union(b))} AI companies')
    return


# This function returns a final dataset on Crunchbase transactions for AI companies to be used in the tables.
def prepare_transactions(cb_vc, cb_ma, cb_ai, ref_ai):
    cb_vc = cb_vc[['year', 'target_country', 'target_name', 'target_uuid',
             'investment_value', 'investment_value_median', 'target_province',
             'investor_name', 'investor_uuid', 'investor_country', 'China_inv', 'Target_Region', 'investor_Region',
             'investor_province', 'MA', 'Crunchbase_ID_target', 'round_id', 'investment_type']]
    cb_ma = cb_ma[['year', 'target_country', 'target_name', 'target_uuid', 'Target_Region', 'investor_Region',
             'investment_value', 'target_valuation', 'target_province',
             'investor_name', 'investor_uuid', 'investor_country', 'round_id',
             'investor_province', 'MA', 'investment_value_median', 'China_inv', 'Crunchbase_ID_target']]
    # get application code
    AI_comp_join = cb_ai[['Crunchbase_ID', 'application_code']].drop_duplicates()
    # merge Refinitiv and CB companies
    AI_comp_join = AI_comp_join[['application_code', 'Crunchbase_ID']].append(
        ref_ai[['Crunchbase_ID', 'application_code']].loc[ref_ai['Crunchbase_ID'].notna()]).drop_duplicates()
    ### Save AI company list if needed.
    AI_comp_join.to_excel('data/AI_company.xlsx')
    # Keep only AI companies
    cb_vc = cb_vc.loc[cb_vc['Crunchbase_ID_target'].isin(AI_comp_join['Crunchbase_ID'].values)]
    # Add patch to match companies by Crunchbase_ID and by target_uuid for VC data
    # merge do the same patch with the MA data
    cb_ma = cb_ma.loc[cb_ma['Crunchbase_ID_target'].isin(AI_comp_join['Crunchbase_ID'].values)]
    # Get stat for unique companies with transactions
    vc_ai_ncomp_ID = cb_vc['Crunchbase_ID_target'].nunique()
    ma_ai_ncomp_ID = cb_ma['Crunchbase_ID_target'].nunique()
    # create joint table of VC and MA transactions
    inv = cb_vc.append(cb_ma, sort=True)
    # merge with application code
    inv = inv.merge(AI_comp_join[['Crunchbase_ID', 'application_code']], left_on='Crunchbase_ID_target',
                    right_on='Crunchbase_ID', how='left')
    # recode MA and VC codes
    print('Total investment value by MA/VC')
    inv = inv.loc[(inv.year > 2014) & (inv.year < 2020)]
    # print investment statistics
    joint_inv_st(inv, ref_ai, cb_vc, cb_ma, vc_ai_ncomp_ID, ma_ai_ncomp_ID, cb_ai)
    inv.to_csv('data/inv.csv')
    return inv
