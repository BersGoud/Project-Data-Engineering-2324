from prefect import task, flow
import pandas as pd
import psycopg2
import boto3
import os

# PostgreSQL connection details
DB_HOST = "192.168.1.4"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "Newpassword"

# S3 details
S3_BUCKET_NAME = 'dataengineering-bers-2324'
S3_REGION = 'us-east-1'

@task
def export_to_parquet(table_name, schema='dw'):
    """
    Export a PostgreSQL table to a Parquet file.
    """
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    
    query = f'SELECT * FROM {schema}.{table_name}'
    df = pd.read_sql(query, conn)
    
    # Define the Parquet file path
    parquet_file = f'{table_name}.parquet'
    
    # Export to Parquet
    df.to_parquet(parquet_file, engine='pyarrow')
    
    # Close the connection
    conn.close()
    
    return parquet_file

@task
def upload_to_s3(file_path, bucket_name, region_name):
    """
    Upload a file to an S3 bucket.
    """
    s3_client = boto3.client('s3', region_name=region_name)
    file_name = os.path.basename(file_path)
    
    # Upload the file
    s3_client.upload_file(file_path, bucket_name, file_name)
    
    # Optionally, delete the local file after uploading
    os.remove(file_path)

@flow
def export_and_upload_tables():
    """
    Export PostgreSQL tables to Parquet files and upload them to S3.
    """
    tables = [
        'vlucht_fct', 'luchthaven_dim', 'vliegtuig_dim', 'weer_dim', 'klant_dim', 'maatschappij_dim'
    ]
    
    # Export each table and upload to S3
    for table in tables:
        parquet_file = export_to_parquet(table)
        upload_to_s3(parquet_file, S3_BUCKET_NAME, S3_REGION)
