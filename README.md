# Enhanced ETL Workflow with Python, AWS S3, RDS, and Glue

## Overview
This project demonstrates an ETL (Extract, Transform, Load) pipeline implemented using Python, AWS services (S3 and RDS), and a variety of data transformation techniques. The pipeline is responsible for downloading raw data, transforming it, and loading it into a MySQL database hosted on AWS RDS.

## Key Features:
*   Download data from a given URL.
*   Extract data from a ZIP file.
*   Transform data, including unit conversions (height to meters, weight to kilograms).
*   Load transformed data into an RDS MySQL database.
*   Use of AWS S3 for temporary data storage and transfer.
*   Log generation for tracking pipeline execution.

## Prerequisites
Before running the ETL pipeline, make sure you have the following setup:

1. Python and Libraries

2. AWS Setup
An AWS account with access to S3 and RDS services.
S3 bucket: A bucket (data-for-etl-project) for uploading and downloading files.
RDS MySQL instance: A MySQL database instance hosted on AWS RDS. Ensure that your RDS instance is accessible from your local environment or where the script will run (check security groups and VPC settings).

3. Log Directory
The script creates a Logs directory in your working directory for storing the logs of each ETL run.

## Steps to Run the ETL Pipeline
1. Download the Dataset
The pipeline begins by downloading a ZIP file from a given URL using wget.

2. Extract the ZIP File
The downloaded ZIP file is then extracted using Python's built-in zipfile module.

3. Upload Raw Data to S3
The extracted files (CSV, JSON, or XML) are uploaded to an AWS S3 bucket.

4. Download Data from S3
The script will list all files in the S3 bucket under the specified prefix (datastore/), filter the valid data formats, and download them locally for processing.

5. Data Transformation
The pipeline reads the downloaded files and applies transformations such as:

Unit conversion (e.g., height from inches to meters, weight from pounds to kilograms).
The transformed data is saved as a new CSV file.

6. Load Data into AWS RDS
Finally, the transformed data is uploaded into a MySQL database hosted on AWS RDS. The data is inserted into a table (transformed_data).


##  Logging
The pipeline logs all activities, including successful operations and errors. The logs are stored in etl_pipeline.log under the Logs directory.
