# This file copies of the raw data from Crunchbase and Refinitiv to BQ and storage bucket #
import os
import subprocess
from oauth2client.client import GoogleCredentials
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/ir177/Documents/ID/GCP-CSET Projects-49aa9c25f835 all admin.json"
from helpers.gcs_storage import list_blobs, delete_blob, download_blob, upload_blob
from google.cloud import storage, bigquery
from os import listdir
import re
import sqlvalidator
from datetime import date
import pandas as pd
from google.cloud import bigquery
import google.auth
import textwrap

credentials, project = google.auth.default(scopes=[
    'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/devstorage.full_control'])
client = bigquery.Client(project='gcp-cset-projects', credentials=credentials)

# matched_companies_id has unique PermID_ref we don;t care about other duplicates */

def add_acc_message(s):
    # write code processing information and save it to debug file
    acc_message = '\n' +  '\n'.join(textwrap.wrap(s,120))
    print('\n'.join(textwrap.wrap(s,120)))
    with open('debug.txt', 'a') as file:
        file.write(acc_message)

# runs a sql querry query_dic[name] and saves results in private_ai_investment dataset and bucket.
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

# load raw data table used in the estimation
sql_rawdata = {
    'organizations' : 'select * from gcp_cset_crunchbase.organizations',
    'organization_descriptions' : 'select * from gcp_cset_crunchbase.organization_descriptions ',
    'CB_Ref_match' : 'select * except(cb_url) from (select * from zach_companies.CB_Ref_match) m left join '
                     '(select uuid as CB_UUID, cb_url from gcp_cset_crunchbase.organizations) o ON '
                     'm.Crunchbase_ID = o.cb_urlh',
    'PERMID' : 'select * from gcp_cset_tr_refinitiv.PERMID ',
    'Target' : 'select * from gcp_cset_tr_refinitiv.Target ',
    'applications' : 'select * from zach_companies.applications',
    'acquisitions' : 'select * from gcp_cset_crunchbase.acquisitions',
    'ipos' : 'select * from  gcp_cset_crunchbase.ipos',
    'org_parents' : 'select * from  gcp_cset_crunchbase.org_parents',
    'funding_rounds' : 'select * from  gcp_cset_crunchbase.funding_rounds',
    'investments' : 'select * from  gcp_cset_crunchbase.investments',
    'investors' : 'select * from  gcp_cset_crunchbase.investors',
    'MA' : 'select * from  gcp_cset_tr_refinitiv.MA',
    'MA_Org' : 'select * from  gcp_cset_tr_refinitiv.MA_Org',
    'INVDTL' : 'select * from  gcp_cset_tr_refinitiv.INVDTL',
    'FIRM' : 'select * from  gcp_cset_tr_refinitiv.FIRM'
    }

# run the update of the raw data:
def update_raw():
    for k in sql_rawdata:
        add_acc_message(f'Copying raw data {k} private_ai_investmnet BQ dataset')
        BQstorageQ(k, sql_rawdata, client)
    return












