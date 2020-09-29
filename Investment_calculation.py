# This scripts calculates private investment in AI statistics for CSET report XXX
# If requires access to the Google Cloud in CSET where the data is stored
# Outside views should use it to understand how the data was transformed in the report, but it's imposible to
# replicate without access to GCP.

# impor libraries
import argparse
from datetime import date
import pandas as pd
# import BQ user defined functions
from querry_from_bq import clean_sql_Q, run_BQ, load_val_data
# import function to update raw data
from raw_data_freeze import update_raw
# import google functions
from helpers.gcs_storage import list_blobs, delete_blob, download_blob, upload_blob
# import helpmainer  functions
from helpers.functions import load_clean, overlap, prepare_transactions, table_func, dict_test,\
    validation_match, clean_country,  impute_missing_values, clean_VC_after_MA, add_acc_message
# import additional misc function
from additional_tests import China_qrs, mis_inv, average_deal, cb_ref_match_stat, add_tests, mask



if __name__ == "__main__":
    # load arguments from terminal
    parser = argparse.ArgumentParser()
    # parse argument whether we need to update analysis data
    parser.add_argument('--update_analysis_data',  action='store_true', required=False,  help="Download data from GCP to hard drive.")
    # parse argument whether we need to update raw data on GCP. Don't use unless you want to update the report using
    # more recent data.
    parser.add_argument('--update_raw_data',  action='store_true', required=False, help="Update data in GCP. RARELY USED.")
    args = parser.parse_args()
    # get today's date
    curdate = date.today().strftime("%Y%m%d")
    # start a debug file
    with open('debug.txt', 'w') as file:
        file.write(f'Start running private investment in ai code on {curdate}')
    # update raw data if needed
    if args.update_raw_data:
        add_acc_message("Raw data is updated in BQ and Storage bucket.")
        update_raw()
    # update analysis data on hard-drive if needed
    if args.update_analysis_data:
        add_acc_message("Analysis data is updated in BQ and Storage bucket.")
        query_dic = clean_sql_Q()# load and clean SQL querries
        run_BQ(query_dic)# Run querries
        load_val_data()# Run fixed data that does need to be updated like validation set
    # Start running the analysis
    print("Load and clean transactions data")
    cb_ma, cb_vc, ref_vc, cb_ai, ref_ai, ref_ma,cb_ref_match = load_clean()
    # Check the company coverage with validation dataset:
    validation_match(cb_ai, ref_ai)
    # recode countries
    cb_vc, cb_ma, ref_vc = clean_country(cb_vc, cb_ma, ref_vc)
    # Calculate overlap between CB and Ref
    print("Calculate and save overlap data")
    overlap(ref_ai, cb_vc, cb_ai, ref_vc, ref_ma, cb_ref_match)
    # impute missing values:
    print("Impute missing values for VC and MA transactions")
    cb_ma, cb_vc = impute_missing_values(cb_vc, cb_ma)
    # additional data cleaning
    cb_vc, cb_ma, ref_vc = clean_VC_after_MA(cb_vc, cb_ma, ref_vc)
    print("Prepare investment statistics for Crunchbase data")
    inv = prepare_transactions(cb_vc, cb_ma, cb_ai, ref_ai)
    print("Run summary statistics")

    # Main tables in the report to be saved to excel
    main_tab_list = ['target_total_MA', 'target_total_MA_med',  'target_total', 'target_total_med', 'total_or_China_MA_med',
        'total_or_China_med', 'count_tot', 'count_MA', 'total_China', 'total_China_MA', 'total_or_China_med',
        'total_or_China_MA_med', 'count_or_China_MA','count_or_China', 'aver_disc', 'aver_disc_MA']
    # dict for sheets + results
    # create dictionary from the main list. The values are the aggregate table
    sheet_names_main_dic = table_func(main_tab_list, inv)
    # test if there are two sheet with the same results (common error).
    dict_test(sheet_names_main_dic)
    # save results to Excel file
    with pd.ExcelWriter('data/CB_AI_investment.xlsx') as writer:  # doctest: +SKIP
        for key in sheet_names_main_dic:
            sheet_names_main_dic[key].to_excel(writer, sheet_name=key)
    # Generate results for large investments [>$100 million]
    sheet_names_large_dic = table_func(main_tab_list, inv.loc[inv['investment_value']>100])
    # test results
    # save results for large companies
    dict_test(sheet_names_large_dic)
    with pd.ExcelWriter('data/CB_AI_investment_large.xlsx') as writer:  # doctest: +SKIP
        for key in sheet_names_large_dic:
            sheet_names_large_dic[key].to_excel(writer, sheet_name=key)

    ################################ Run statistics of Applications
    # set security related application codes
    security_app_code = [1,3,4,7,8,11,15,16,17]
    # create security application indicator
    inv['security'] = inv['application_code'].apply(lambda x: 1 if x in security_app_code else 0)
    # Run applications data
    tab_names = ['target_total_app', 'target_total_sec', 'target_total_app_med', 'target_total_sec_med', 'count_total_app',
                 'count_total_sec','target_total_app_China', 'target_total_sec_China', 'target_total_app_med_China',
                 'target_total_sec_med_China', 'count_total_app_China', 'count_total_China_sec']
    # create dictionary for application results
    sheet_names_app_dic = table_func(tab_names, inv)
    # test if there are two sheet with the same results (common error).
    dict_test(sheet_names_app_dic)
    # save investment by application to Excel
    with pd.ExcelWriter('data/CB_AI_investment_app.xlsx') as writer:
        for key in sheet_names_app_dic:
            sheet_names_app_dic[key].to_excel(writer, sheet_name=key)


    # Miscellaneous additional tests

    # Test of overlpa between Crunchbase and Refinitiv
    cb_ref_match_stat()
    # Descriptions of China specific deals
    China_qrs(inv)
    # Tabluating missing investment values
    mis_inv(ref_ai, cb_ai, inv)
    # Average deal size
    average_deal(inv)
    # Calculate missing VC and MA deals
    add_tests(inv)
    # save masked IDs for public replication
    mask(inv)
    # export files to GCP
    exp_f = ['CB_Ref_Overlap_stat.xlsx', 'joint_CB_Ref_transactions.csv', 'overlap_transactions.csv',
             'CB_AI_investment_large.xlsx','CB_AI_investment_app.xlsx', 'CB_AI_investment.xlsx', 'add_test.xlsx', 'masked_inv.csv']
    print("Upload results to GCP")
    for f in exp_f:
        upload_blob('private_ai_investment',  'data/' + f, 'results/' + f,)
    # The End