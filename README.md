# AWS Glue ETL Pipeline for Sales Data Processing and Redshift Integration

## Overview
This project is an AWS Glue-based ETL (Extract, Transform, Load) pipeline that processes sales data from CSV files stored in Amazon S3, performs various data cleaning and transformation tasks using Apache Spark (via PySpark), and loads the refined data into Amazon Redshift for downstream analytics and reporting.

##  Features
- Reads raw CSV files from S3 using AWS Glue and PySpark.
- Adds source metadata (filename) to each record.
- Cleans and transforms fields including date/time formatting and column renaming.
- Performs basic exploratory data analysis:
  - Schema inspection
  - Record count
  - Sample previews
- Computes summary statistics such as:
  - Total sales by city
  - Average rating by product line
- Filters data by criteria (e.g., high sales, payment method).
- Conducts time-based analysis (e.g., monthly and branch-wise sales).
- Writes processed data to Redshift staging and main tables.
- Uses `pg8000` to handle Redshift operations like `TRUNCATE`, `UPDATE`, and `INSERT`.

## Architecture
```plaintext
S3 (CSV files)
   ↓
AWS Glue (PySpark ETL)
   ↓
Data Cleaning, Transformation & Aggregation
   ↓
Amazon Redshift (Staging Table → Main Table)

##  Technologies used
- AWS Glue (ETL & job orchestration)
- Apache Spark / PySpark (Data processing)
- Amazon S3 (Data storage)
- Amazon Redshift (Data warehouse)
- pg8000 (Redshift JDBC Python client)

## Setup Instructions
Upload your CSV data to the specified S3 bucket path:
s3://s3bucketname/bucketfolder/

## Configure Redshift connection parameters in the script:
Update the following variables:
USERNAME = 'your-redshift-username'
PASSWORD = 'your-password'
REDSHIFT_DATABASE_NAME = 'your-db-name'
HOST = 'your-redshift-host'
REDSHIFT_ROLE = 'arn:aws:iam::1234567890:role/your-role'
REDSHIFT_JDBC_URL = 'your-jdbc-url'
redshift_tmp_dir = 's3://your-temp-dir/'

Deploy the script in AWS Glue as a PySpark job.
Execute the job via the AWS Glue console, CLI, or trigger-based workflow.

## Key Functions
Function                                Purpose
---------                               --------
main()	                                Orchestrates the ETL flow.
readcsvfiles()	                        Reads, transforms, and cleans raw CSV data from S3.
basicexploratoryanalysis()	            Performs basic schema and data inspection.
summarystats_dataaggregation()	        Generates summary stats and group-wise aggregations.
datafilteration()	                      Filters data based on conditions like sales amount and payment type.
timebasedanalysis()	                    Aggregates sales over time and by branch.
writetoredshiftdbfromstaging()	        Loads the final DataFrame into Redshift via staging and upserts.

## Security & IAM
- Ensure IAM roles assigned to Glue have permissions to:
-- Read from S3
-- Write to Redshift
-- Assume necessary Redshift access policies
