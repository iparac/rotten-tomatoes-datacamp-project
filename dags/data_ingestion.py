
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from google.cloud import storage, bigquery
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.operators.bash import BashOperator
from airflow import DAG, settings
from airflow.models import Connection
import json
import sqlite3
import sqlalchemy 


def separate_tables():

    #Now in order to read in pandas dataframe we need to know table name
    # cursor = conn.cursor()
    # cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    # print(f"Table Name : {cursor.fetchall()}")
    # Table Name : [('rotten_tomatoes',), ('critic_reviews',), ('user_reviews',)]
    conn = sqlite3.connect("datasets/rotten-tomatoes-full.db")    
    
    df_critic_reviews = pd.read_sql_query('SELECT * FROM critic_reviews ', conn, index_col='index')
    df_user_reviews = pd.read_sql_query('SELECT * FROM user_reviews ', conn, index_col='index')
    
    df_critic_reviews.to_csv('datasets/critic_reviews.csv')
    df_user_reviews.to_csv('datasets/user_reviews.csv')
    conn.close()

    return(df_critic_reviews, df_user_reviews)


default_args={
    'owner':'Ivan',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}


GCS_BUCKET = 'rotten-tomatoes-bucket'
GCS_OBJECT_PATH = 'rotten_tomatoes_reviews'
SOURCE_TABLE_NAME = 'critic_reviews'
SOURCE_TABLE_NAME2 = 'user_reviews'
BQ_DS = 'rotten_tomatoes_reviews'
BQ_PROJECT = '<PUT YOUR GCP PROJECT ID HERE>'
schema = [
    {
        'name': 'index',
        'type': 'INT64',
        'mode': 'NULLABLE',
    },
    {
        'name': 'id',
        'type': 'string',
        'mode': 'NULLABLE',
    },
    {
        'name': 'timestamp',
        'type': 'STRING',
        'mode': 'NULLABLE',
    },
    {
        'name': 'fresh',
        'type': 'BOOLEAN',
        'mode': 'NULLABLE',
    },
    {
        'name': 'quote',
        'type': 'STRING',
        'mode': 'NULLABLE',
    },
    {
        'name': 'review_link',
        'type': 'string',
        'mode': 'NULLABLE',
    },
        {
        'name': 'critic',
        'type': 'string',
        'mode': 'NULLABLE',
    },
    {
        'name': 'publication',
        'type': 'string',
        'mode': 'NULLABLE',
    },
    {
        'name': 'top_critic',
        'type': 'BOOLEAN',
        'mode': 'NULLABLE',
    },
    {
        'name': 'original_score',
        'type': 'STRING',
        'mode': 'NULLABLE',
    },
    {
        'name': 'title',
        'type': 'STRING',
        'mode': 'NULLABLE',
    },
    {
        'name': 'review',
        'type': 'STRING',
        'mode': 'NULLABLE',
    },
],
schema2 = [
    {
        'name': 'index',
        'type': 'INT64',
        'mode': 'NULLABLE',
    },
    {
        'name': 'id',
        'type': 'STRING',
        'mode': 'NULLABLE',
    },
    {
        'name': 'title',
        'type': 'STRING',
        'mode': 'NULLABLE',
    },
    {
        'name': 'review',
        'type': 'STRING',
        'mode': 'NULLABLE',
    },
    {
        'name': 'score',
        'type': 'FLOAT64',
        'mode': 'NULLABLE',
    },
]



def add_gcp_connection(**kwargs):
    new_conn = Connection(
            conn_id="google_cloud_default",
            conn_type='google_cloud_platform',
    )
    extra_field = {
        "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform",
        "extra__google_cloud_platform__project": "<PUT YOUR GCP PROJECT ID HERE>",
        "extra__google_cloud_platform__key_path": '/usr/local/airflow/dags/keys/gcp-cred.json'
    }

    session = settings.Session()

    #checking if connection exist
    if session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first():
        my_connection = session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).one()
        my_connection.set_extra(json.dumps(extra_field))
        session.add(my_connection)
        session.commit()
    else: #if it doesn't exit create one
        new_conn.set_extra(json.dumps(extra_field))
        session.add(new_conn)
        session.commit()



def upload_to_bucket(blob_name, path_to_file, bucket_name):
    storage_client = storage.Client.from_service_account_json('/usr/local/airflow/dags/keys/gcp-cred.json')
    bucket = storage_client.get_bucket(bucket_name)

    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)
    return blob.public_url

# delete all buckets before importing a new one (to save on google cloud costs)
def delete_blob_from_bucket(bucket_name):
    storage_client = storage.Client.from_service_account_json('/usr/local/airflow/dags/keys/gcp-cred.json')
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='snp')
    for blob in blobs: 
       blob.delete()




def create_postgres_engine(user,password,host,port,db,table_name,dataframe):     

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    conn = psycopg2.connect(user=user, password=password, host=host, port=port, database=db)
    cursor = conn.cursor()
    query = """CREATE SCHEMA IF NOT EXISTS rotten_tomatoes;"""
    cursor.execute(query)

    dataframe.to_sql(name=table_name, con = engine, index ='write_index', chunksize=10000, schema='public', if_exists='replace')




def ingest_pipeline():
    
    # separate tables from the db file

    df_critic_reviews, df_user_reviews = separate_tables()

    # push csv to postgres database
    create_postgres_engine('airflow','airflow','postgres',5432,'airflow','critic_reviews', df_critic_reviews)
    create_postgres_engine('airflow','airflow','postgres',5432,'airflow','user_reviews', df_user_reviews)



def push_to_bucket():
    
    # upload to google cloud bucket  
    upload_to_bucket('critic_reviews.csv', 'datasets/critic_reviews.csv' ,'rotten-tomatoes-bucket')
    upload_to_bucket('user_reviews.csv', 'datasets/user_reviews.csv' ,'rotten-tomatoes-bucket')



def main():    
    # API calls -> final csv
    ingest_pipeline()



schedule_interval = "00 10 * * *"
with DAG(
  'Rotten-Tomattoes-Reviews',
  default_args=default_args,
  start_date=days_ago(1),
  schedule_interval=schedule_interval,
  tags=['movies','critic_reviews']
) as dag:
    auth_gcp = PythonOperator(
    task_id='add_gcp_connection_python',
    python_callable=add_gcp_connection
    )
    push_to_postgres = PythonOperator(
        task_id='ingest_and_push_to_postgres',
        python_callable=main
    )
    push_to_gcp_bucket = PythonOperator(
        task_id='postgres_to_gcp_bucket',
        python_callable=push_to_bucket
    )
    push_to_gcp_bigquery_table1 = GCSToBigQueryOperator(
    task_id=f'gcs_to_bq_critic',
    bucket=GCS_BUCKET,
    source_objects=[f'{SOURCE_TABLE_NAME}.csv'],
    destination_project_dataset_table='.'.join(
        [BQ_PROJECT, BQ_DS, SOURCE_TABLE_NAME]
    ),
    schema_fields=schema,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    allow_quoted_newlines=True,
    )
    push_to_gcp_bigquery_table2 = GCSToBigQueryOperator(
    task_id=f'gcs_to_bq_user',
    bucket=GCS_BUCKET,
    source_objects=[f'{SOURCE_TABLE_NAME2}.csv'],
    destination_project_dataset_table='.'.join(
        [BQ_PROJECT, BQ_DS, SOURCE_TABLE_NAME2]
    ),
    schema_fields=schema2,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    allow_quoted_newlines=True,
    )
    delete_used_bucket = GCSDeleteObjectsOperator(
    task_id='delete_used_csv_from_gcp_bucket',
    bucket_name=GCS_BUCKET,
    objects=[f'{SOURCE_TABLE_NAME}.csv', f'{SOURCE_TABLE_NAME2}.csv'],
    )
    run_dbt = BashOperator(
    task_id='run_dbt_transformations', 
    bash_command="pip install dbt-bigquery && cd /opt/airflow/rotten_project_dbt && dbt run"
    )
auth_gcp >> push_to_postgres >> push_to_gcp_bucket >> [push_to_gcp_bigquery_table1, push_to_gcp_bigquery_table2]  >> delete_used_bucket >> run_dbt


