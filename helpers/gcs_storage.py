from google.cloud import storage
from google.cloud import storage, bigquery
# uploads blob from GCP buckets
from google.oauth2 import service_account
from google.cloud import bigquery
import google.auth
from google.oauth2.service_account import Credentials
import os


scopes=[ 'https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/bigquery', 'https://www.googleapis.com/auth/cloud-platform']

#authenticate you GCP account here as credentials.json"

#if os.path.exists(credentials.json):
#    credentials = Credentials.from_service_account_file(credentials.json)
#    credentials = credentials.with_scopes(scopes)
#    client = bigquery.Client(credentials=credentials)
#else:
#    print("Update authentication method in gcs_storage.py")

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))


# downloads blob from GCP
def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print('Blob {} downloaded to {}.'.format(
        source_blob_name,
        destination_file_name))


def delete_blob(bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()
    print('Blob {} deleted.'.format(blob_name))



def list_blobs(bucket_name, prefix):
    """Lists all the blobs in the bucket."""
    from google.cloud import storage
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    print(blobs)
    tlist = []
    for blob in blobs:
        if prefix in blob.name:
            tlist.append(blob.name)
    return tlist

# # Construct a BigQuery client object.
# client = bigquery.Client()
#
# # table_id = 'your-project.your_dataset.your_table'
#
# # If the table does not exist, delete_table raises
# # google.api_core.exceptions.NotFound unless not_found_ok is True.
# client.delete_table(table_id, not_found_ok=True)  # Make an API request.
# print("Deleted table '{}'.".format(table_id))

def BQ_to_bucket(bucket, dataset, tablebq, name, folder, sql_dic):
    client = bigquery.Client()
    dellist = list_blobs(bucket, folder + '/' + name)
    for h in range(0,len(dellist)):
        delete_blob(bucket, dellist[h])
    job_config = bigquery.QueryJobConfig(destination_format="CSV")
    table_ref = client.dataset(dataset).table(tablebq)
    job_config.destination = table_ref
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    query_job = client.query(
        sql_dic[f'{name}'],
        location='US', # Location must match dataset
        job_config=job_config)
    rows = list(query_job)  # Waits for the query to finish
    # Export table to GCS
    destination_uri = f"gs://{bucket}/{folder}/{name}_*.csv"
    dataset_ref = client.dataset(dataset, project="gcp-cset-projects")
    table_ref = dataset_ref.table(f"{tablebq}")
    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location='US')
    extract_job.result()  # Waits for job to complete
