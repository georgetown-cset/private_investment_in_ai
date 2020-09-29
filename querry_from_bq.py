# This script querries the data from BQ and updated the files used for analysis in the Google cloud.
# Only run if you want to add new data to the analysis!
import os
from helpers.gcs_storage import list_blobs, delete_blob, download_blob, upload_blob
from google.cloud import storage, bigquery
from os import listdir
import sqlvalidator
from datetime import date
import pandas as pd
from google.cloud import bigquery
import google.auth
import textwrap
# GCP credentials
credentials, project = google.auth.default(scopes=['https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/bigquery','https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/devstorage.full_control'])
client = bigquery.Client(project='gcp-cset-projects', credentials=credentials)
# matched_companies_id has unique PermID_ref we are less concerned about othe# set data accounting messager duplicates */

def add_acc_message(s):
    acc_message = '\n' +  '\n'.join(textwrap.wrap(s,120))
    print('\n'.join(textwrap.wrap(s,120)))
    with open('debug.txt', 'a') as file:
        file.write(acc_message)

def BQstorageQ(name, query_dic, client):
    # set current data
    curdate = date.today().strftime("%Y%m%d")
    # extract querry text
    querry_text = query_dic[name]
    # Run querry. Save results for archive with a date and latest version in BQ and bucket
    for bq_tname in [f'{name}_{curdate}', f'{name}_latest']:
        # save raw data to BQ table
        table_ref = client.dataset("private_ai_investment").table(bq_tname)
        job_config = bigquery.QueryJobConfig()
        job_config.destination=table_ref
        job_config.write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE
        query_job = client.query(querry_text,job_config=job_config,location="US")
        query_job.result()
        # check if Querry has error
        if query_job.errors != None:
            add_acc_message(f"Querry {name} produced errors: \r\n {query_job.errors}")
        job_config = bigquery.job.ExtractJobConfig(destination_format="CSV")
        # Upload data to google bucket
        table_ref = client.dataset("private_ai_investment").table(bq_tname)
        if bq_tname == f'{name}_{curdate}':
            destination_uri = f"gs://private_ai_investment/input/{bq_tname}_*.csv"
        else:
            destination_uri = f"gs://private_ai_investment/input_latest/{bq_tname}_*.csv"
        extract_job = client.extract_table(table_ref, destination_uri, location='US', job_config=job_config)
        extract_job.result()
        print(list_blobs('private_ai_investment', f'input_latest/{name}_latest_'))
    return

def clean_sql_Q():
# read the list of sql querries from the folder
    sql_files_list = listdir('sql')
    # list the files we read
    add_acc_message(f'\r\n We have read the following sql querries from the sql folder: {sql_files_list}')
    # load querries in dic:
    query_dic = {}
    for file in sql_files_list:
        if file.startswith("."):
            continue
        # read query
        f = open(f"sql/{file}", "r").read()
        if "gcp_cset_crunchbase" in f:
            add_acc_message(f'Found reference to dynamic data gcp_cset_crunchbase in {file} querry')
        if "gcp_cset_tr_refinitiv" in f:
            add_acc_message(f'Found reference to dynamic data tr_refinitiv in {file} querry')
        sql_query = sqlvalidator.parse(f)
        if not sql_query.is_valid():
            print(sql_query.errors)
            add_acc_message(f'Found SQL validation error in the querry {file}')
        # add querry to the dic
        query_dic.update({file[:-4]:f})
    return query_dic

def data_QC(df, tab_name):
    # find unique identifier:
    max_unique = 0
    add_acc_message(f' \r\n\r\n Running data QC for table {tab_name}.')
    for c in df.columns:
        # Check if all values in the column are missing to raise an warning:
        if len(df.loc[df[c].isna()]) == len(df):
            add_acc_message(f' \r\n WARNING: Column {c} in table {tab_name} has all values missing.')
        # calculate the number of unique values
        num_unique = df[c].nunique()
        if num_unique > max_unique:
            unique_id = c
            max_unique = num_unique
    # found: unique_id is the name of unique identifier. max_unique is the number of unique values
    if df.duplicated().sum() > 0:
        add_acc_message(f' \r\n WARNING: Found {df.duplicated().sum()} duplicated rows in table '
            f'{tab_name}. The rows are saved in \"debug/{tab_name}_duplicated_rows.csv\"')
        # save duplicated records:
        df.loc[df.duplicated(keep=False)].to_csv(f'debug/{tab_name}_duplicated_rows.csv')
    # check if the ID is not unique among non-duplicate records:
    if df[~df.duplicated()][unique_id].nunique() < len(df[~df.duplicated()]):
        add_acc_message(f' \r\n WARNING: ID is not Unique. Likely ID is the columns {unique_id}.'
            f' Found {df[~df.duplicated()][unique_id].nunique()} unique IDs in table {tab_name} with '
            f'{len(df[~df.duplicated()])} rows. Duplicated IDs are saved in table. The rows are saved in \"debug/{tab_name}'
                                                   f'_duplicated_ids.csv\"')
        df.loc[df[unique_id].duplicated(keep=False), ].to_csv(f'debug/{tab_name}_duplicated_ids.csv')
    else:
        add_acc_message(f"In the table {tab_name} column {unique_id} is the unique ID.")
    return

# Upload citations to BQ and Bucket:
def run_BQ(query_dic):
    for k in query_dic:
    # The line above runs a BQ, comment if you need a fast run.
        print(f"Running querry for {k}")
        BQstorageQ(k, query_dic,  client)
        filelist = list_blobs('private_ai_investment', f'input_latest/{k}_latest_')
        print("Downloading filelist")
        for j in range(0,len(filelist)):
            download_blob('private_ai_investment', filelist[j],  f"data/{filelist[j]}")
        # drop refr
        all_filenames = [w for w in filelist]
        # combine all files in the list
        print(all_filenames)
        combined = pd.concat([pd.read_csv(f'data/{f}') for f in all_filenames])
        # delete downloaded files
        # delete files
        for fdel in filelist:
            os.remove(f'data/{fdel}')
        combined.to_csv('data/'+k+'.csv', index=False)
        data_QC(combined, k)
        # upload the merged results ready to analysis to GCP
        today = date.today().strftime("_%Y_%m_%d_")
        # getting timeout issues using the client library and don't want to figure out a way to work around them right now
        # when I know this will work, shelling out is not great practice though
        #subprocess.run(f"gsutil cp data/{k}.csv gs://private_ai_investment/analysis/{k}.csv".split(), check=True)
        #subprocess.run(f"gsutil cp data/{k}.csv gs://private_ai_investment/input/{k}_{today}.csv".split(), check=True)
        upload_blob('private_ai_investment', f'data/{k}.csv', f'analysis/{k}.csv')
        upload_blob('private_ai_investment', f'data/{k}.csv', f'input/{k}_{today}.csv')
    return

# load validation data:
def load_val_data():
    fix_list = ['validation_set_v1', 'validation_set_v2']
    for fl in fix_list:
        download_blob('private_ai_investment', f'fixed_input/{fl}.csv', f'data/{fl}.csv')





