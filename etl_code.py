import os
import subprocess
import wget 
import zipfile
import boto3
import pandas as pd
from sqlalchemy import create_engine
import logging
import credentials

# Define the directory for logging 
log_dir = r"C:\Users\renor\OneDrive\Desktop\ETL_project\Logs"
 
# Create the directory if it doesn't exist 
os.makedirs(log_dir, exist_ok=True) 

# Define the log file path 
log_file = os.path.join(log_dir, 'etl_pipeline.log') 

# Set up logging configuration
logging.basicConfig(filename=log_file, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the S3 client
s3_client = boto3.client('s3')
bucket_name = 'data-for-etl-project'

# Step 1: Download the file using wget
def download_file(url, download_path):
    logging.info("Downloading dataset using wget...")
    try:
        subprocess.run(["wget", "-O", download_path, url], check=True)
        logging.info("Dataset downloaded successfully")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error downloading dataset: {e}")
        exit(1)

# Step 2: Extract the file from the zip folder
def extract_file(download_path, extract_path):
    logging.info("Extracting files using Python's zipfile module...")
    try:
        with zipfile.ZipFile(download_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        logging.info(f"Files extracted successfully to: {extract_path}")
    except zipfile.BadZipFile as e:
        logging.error(f"Error extracting ZIP file: {e}")
        exit(1)

# Unified S3 functions
def upload_to_s3(file_path, bucket_name, s3_key):
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        logging.info(f'Successfully uploaded {file_path} to s3://{bucket_name}/{s3_key}')
    except Exception as e:
        logging.error(f'Error uploading {file_path} to S3: {e}')

def list_and_filter_s3_files(bucket_name, prefix, extensions=('.csv', '.json', '.xml')):
    s3_files = []
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            all_files = [item['Key'] for item in response['Contents']]
            s3_files = [f for f in all_files if f.endswith(extensions)]
        else:
            logging.warning(f'No files found in bucket {bucket_name} with prefix {prefix}.')
    except Exception as e:
        logging.error(f'Error listing files in bucket {bucket_name}: {e}')
    return s3_files

def download_from_s3(bucket_name, s3_key, download_path):
    try:
        s3_client.download_file(bucket_name, s3_key, download_path)
        logging.info(f'Successfully downloaded s3://{bucket_name}/{s3_key} to {download_path}')
    except Exception as e:
        logging.error(f'Error downloading s3://{bucket_name}/{s3_key}: {e}')

# Step 3: Process and transform the data
def process_file(file_path):
    logging.info(f"Processing file: {file_path}")
    file_extension = os.path.splitext(file_path)[1].lower()
    if file_extension == '.csv':
        df = pd.read_csv(file_path)
    elif file_extension == '.json':
        df = pd.read_json(file_path, lines=True)
    elif file_extension == '.xml':
        df = pd.read_xml(file_path)
    else:
        logging.error(f"Unsupported file format: {file_extension}")
        raise ValueError(f"Unsupported file format: {file_extension}")

    # Perform unit conversions if columns exist
    if 'height' in df.columns:
        df['height_m'] = (df['height'] * 0.0254).round(2)
    if 'weight' in df.columns:
        df['weight_kg'] = (df['weight'] * 0.453592).round(2)
    
    return df

def transform_files(file_paths, output_file_path):
    logging.info("Transforming files...")
    combined_df = pd.DataFrame()
    for file_path in file_paths:
        df = process_file(file_path)
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    combined_df.to_csv(output_file_path, index=False)
    logging.info(f'Transformed data saved to: {output_file_path}')
    upload_to_s3(output_file_path, bucket_name, 'transformed/transformed_data.csv')

# Step 4: Load transformed data into RDS
def load_into_rds(output_file_path):
    cred = credentials.conn_detail
    try:
        engine = create_engine(f"mysql+pymysql://{cred['username']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['database']}")
        df = pd.read_csv(output_file_path)
        table_name = 'transformed_data'
        df.to_sql(table_name, engine, index=False, if_exists='replace')
        logging.info(f'Successfully uploaded data to RDS table: {table_name}')
    except Exception as e:
        logging.error(f'Error uploading data to RDS: {e}')

# Main ETL pipeline
if __name__ == "__main__":
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip"
    download_path = r"C:\Users\renor\OneDrive\Desktop\ETL_project\source.zip"
    extract_path = r"C:\Users\renor\OneDrive\Desktop\ETL_project"
    output_file_path = r"C:\Users\renor\OneDrive\Desktop\ETL_project\transformed_data.csv"

    try:
        # Step 1: Download the file
        download_file(url, download_path)

        # Step 2: Extract the file
        extract_file(download_path, extract_path)

        # Step 3: Upload extracted files to S3
        # List all file and add
        all_files = os.listdir(extract_path)

        # Filter files by extension (CSV, JSON, XML)
        filtered_files = [f for f in all_files if f.endswith(('.csv', '.json', '.xml'))]

        # Upload filtered files to S3
        for raw_file in filtered_files:
            file_path = os.path.join(extract_path, raw_file)
            s3_key = f'datastore/{raw_file}'  # S3 object key
            upload_to_s3(file_path, bucket_name, s3_key)

        
        # Step 4: List and download files from S3
        download_prefix = 'datastore/'
        local_download_dir = os.path.join(extract_path, 's3')
        os.makedirs(local_download_dir, exist_ok=True)
        s3_files = list_and_filter_s3_files(bucket_name, download_prefix)
        for s3_key in s3_files:
            local_file_path = os.path.join(local_download_dir, os.path.basename(s3_key))
            download_from_s3(bucket_name, s3_key, local_file_path)

        # Step 5: Transform the files
        file_paths = [os.path.join(local_download_dir, f) for f in os.listdir(local_download_dir)]
        transform_files(file_paths, output_file_path)

        # Step 6: Load data into RDS
        load_into_rds(output_file_path)

    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
