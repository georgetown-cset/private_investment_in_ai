
# This scripts calculates private investment in AI statistics for CSET report
# "Tracking AI Investment: Initial Findings From the Private Markets" published in September 2020
# This script replicates tables used in the report
# The identifies of the firms are masked and all data is available lin the Github. No access for CSET data holdings is
# required.

# import libraries
from datetime import date
import pandas as pd
from helpers.functions import table_func, dict_test
from additional_tests import China_qrs, average_deal, add_tests


if __name__ == "__main__":
    ## create new debug file:
    curdate = date.today().strftime("%Y%m%d")
    with open('debug.txt', 'w') as file:
        file.write(f'Start running private investment in ai code on {curdate}')
    print("Prepare investment statistics for Crunchbase data")
    inv = pd.read_csv('data/masked_inv.csv')
    print("Run summary statistics")
    # Main tables to be saved to excel
    main_tab_list = ['target_total_MA' , 'target_total_MA_med',  'target_total', 'target_total_med', 'total_or_China_MA_med',
        'total_or_China_med', 'count_tot', 'count_MA', 'total_China', 'total_China_MA', 'total_or_China_med',
                     'total_or_China_MA_med', 'count_or_China_MA',
            'count_or_China', 'aver_disc', 'aver_disc_MA']
    # dict for sheets + results
    sheet_names_main_dic = table_func(main_tab_list, inv)
    # test if there are two sheet with the same results (common error).
    dict_test(sheet_names_main_dic)
    # save results
    with pd.ExcelWriter('data/CB_AI_investment.xlsx') as writer:  # doctest: +SKIP
        for key in sheet_names_main_dic:
            sheet_names_main_dic[key].to_excel(writer, sheet_name=key)
    # Large company results
    # Keep only large trans
    sheet_names_large_dic = table_func(main_tab_list, inv.loc[inv['investment_value']>100])
    # test if there are two sheet with the same results (common error).
    dict_test(sheet_names_large_dic)
    with pd.ExcelWriter('data/CB_AI_investment_large.xlsx') as writer:  # doctest: +SKIP
        for key in sheet_names_large_dic:
            sheet_names_large_dic[key].to_excel(writer, sheet_name=key)
    ################################ Run statistics of Applications
    # set security related application codes
    security_app_code = [1, 3, 4, 7, 8, 11, 15, 16, 17]
    # create security application indicator
    inv['security'] = inv['application_code'].apply(lambda x: 1 if x in security_app_code else 0)
    # Run applications data
    tab_names = ['target_total_app', 'target_total_sec', 'target_total_app_med', 'target_total_sec_med', 'count_total_app',
                 'count_total_sec', 'target_total_app_China', 'target_total_sec_China', 'target_total_app_med_China',
                 'target_total_sec_med_China', 'count_total_app_China', 'count_total_China_sec']
    # create dictionary for application results
    sheet_names_app_dic = table_func(tab_names, inv)
    # test if there are two sheet with the same results (common error).
    dict_test(sheet_names_app_dic)
    # save investment by application to Excel
    with pd.ExcelWriter('data/CB_AI_investment_app.xlsx') as writer:
        for key in sheet_names_app_dic:
            sheet_names_app_dic[key].to_excel(writer, sheet_name=key)
    # Run miscellaneous additional tests
    # Descriptions of China specific deals
    China_qrs(inv)
    # Average deal size
    average_deal(inv)
    # Calculate missing VC and MA deals
    add_tests(inv)

