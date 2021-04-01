from google.cloud import storage
from google.cloud import bigquery
from project_betterhelp import *
import logging

logging.basicConfig(filename='extract.log', level=logging.INFO)


keypath='service_account.json'
Bucketname= 'bh-de-interview-data'
default_uri = "gs://{}/".format(Bucketname)
download_files = []
dataset_name= "bh-interview-data-de.candidate_1584403.conversations"

def create_storage_client(keypath):
    try:
        Client=storage.Client.from_service_account_json(keypath)
        logging.info("Created  storage client connection for {}".format(keypath))
    except Exception as e:
        logging.error("Unable to create storage client with error {}".format(e))
        return
    return Client

def create_bigquery_client(keypath):
    try:
        Client=bigquery.Client.from_service_account_json(keypath)
        logging.info("Created  bigquery client connection for {}".format(keypath))
    except Exception as e:
        logging.error("Unable to create bigquery client with error {}".format(e))
        return
    return Client


def get_file_name(keypath,bucketname):
    Storage_Client=create_storage_client(keypath)
    try:
        bucket=Storage_Client.get_bucket(bucketname)
    except Exception as e :
        logging.error("Unable to get Google cloud bucket with error {}".format(e))
        return

    filename=list(bucket.list_blobs(prefix='member_success/conversations/',delimiter='/'))
    if not filename:
        raise Exception
        return

    for name in filename:
        if name.name.endswith('csv'):
            download_files.append(default_uri+name.name)

    if not download_files :
        raise Exception ("No files to load")

    return download_files

def main(keypath,Bucketname,dataset_name):
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="created_date"
    )

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=time_partitioning

    )
    file_list = get_file_name(keypath,Bucketname)
    bigquery_Client=create_bigquery_client(keypath)
    for  uri in file_list:
        dt = uri.split('/')[-1].rstrip(".csv").split('_')[-1].replace('-','')
        table_id ="{}${}".format(dataset_name,dt)
        load_job = bigquery_Client.load_table_from_uri(uri,table_id, job_config=job_config)  # Make an API request.
        load_job.result()  # Waits for the job to complete.
        destination_table = bigquery_Client.get_table(table_id)
        logging.info("Loaded {} rows.".format(destination_table.num_rows))


if __name__== "__main__":
    main(keypath,Bucketname,dataset_name)