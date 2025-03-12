Uber Data Analytics - Data Engineering Project

Overview

This project involves building an end-to-end data engineering pipeline that processes Uber data using Google Cloud Platform (GCP) services, specifically Apache Airflow for orchestrating the ETL pipeline and BigQuery for data analysis. The goal of this project is to demonstrate the use of GCP's data processing tools and workflows to handle large-scale datasets and gain insights from Uber-related data.

Project Components

Google Cloud VM Instance: Used to set up and run Apache Airflow.
Apache Airflow: Used to automate, schedule, and monitor the ETL process.
BigQuery: Used for data storage and analysis.
Uber Data: The dataset used for analytics.
Setup Instructions

1. Create Google Cloud VM Instance
Go to Google Cloud Console.
Navigate to Compute Engine and create a new VM instance (Ubuntu is recommended).
SSH into the VM instance.
2. Install Apache Airflow
Update the package list and install dependencies:
sudo apt update
sudo apt install -y python3-pip
sudo apt install -y libmysqlclient-dev
Install Apache Airflow using pip:
pip3 install apache-airflow
3. Set Up Apache Airflow
Initialize the Airflow database:
airflow db init
Start the Airflow web server:
airflow webserver -p 8080
Start the Airflow scheduler in another terminal:
airflow scheduler
4. Set Up BigQuery
In the Google Cloud Console, go to BigQuery.
Create a new dataset to store the Uber data.
Create the necessary tables for data storage.
5. Create Airflow DAG for ETL Pipeline
Inside the Airflow instance, create a new DAG for the ETL process to extract data from Uber, transform it, and load it into BigQuery.
Example DAG code (uber_etl.py):
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG('uber_etl', default_args=default_args, start_date=datetime(2025, 1, 1), schedule_interval='@daily') as dag:
    # Extract data from Uber (GCS to BigQuery)
    extract_task = GCSToBigQueryOperator(
        task_id='extract_uber_data',
        bucket_name='your_bucket_name',
        source_objects=['uber_data.csv'],
        destination_project_dataset_table='your_project.your_dataset.uber_table',
        write_disposition='WRITE_TRUNCATE'
    )

    # Load data into BigQuery for analytics
    load_task = BigQueryOperator(
        task_id='load_uber_data',
        sql='''
            SELECT * FROM `your_project.your_dataset.uber_table`
            WHERE ride_date > '2025-01-01'
        ''',
        destination_dataset_table='your_project.your_dataset.analytics_results',
        use_legacy_sql=False
    )

    extract_task >> load_task
6. Run the DAG
Once the DAG is set up, you can trigger it manually via the Airflow UI or wait for the scheduled time.
7. Monitor and Analyze Data in BigQuery
After the data is loaded into BigQuery, you can use SQL queries to analyze the data.
Example SQL query to analyze Uber data:
SELECT
    COUNT(*) AS total_rides,
    AVG(fare_amount) AS average_fare
FROM
    `your_project.your_dataset.uber_table`
Prerequisites

Google Cloud Platform account.
Basic knowledge of Apache Airflow, BigQuery, and ETL concepts.
Uber dataset (available on Kaggle).
Commands Used in Google Cloud VM Instance

Here is a list of common commands that were used for setting up the project on the VM instance:

SSH into the VM instance:
gcloud compute ssh --zone your-zone your-instance-name
Update package list and install Python3 and dependencies:
sudo apt update
sudo apt install -y python3-pip
sudo apt install -y libmysqlclient-dev
Install Apache Airflow:
pip3 install apache-airflow
Initialize Airflow database:
airflow db init
Start Airflow web server:
airflow webserver -p 8080
Start Airflow scheduler:
airflow scheduler
Upload data to Google Cloud Storage (if needed for extraction):
gsutil cp uber_data.csv gs://your_bucket_name/
Check BigQuery tables:
bq ls your_project:your_dataset

Architecture Overview:
Uber Data Source: The Uber data is stored as CSV or other formats in a Google Cloud Storage (GCS) bucket.
Apache Airflow (on GCP VM): Orchestrates the ETL pipeline. Airflow extracts data from GCS, processes it, and loads it into BigQuery.
Google Cloud Storage (GCS): Holds the raw Uber data.
BigQuery: Stores the transformed data and is used for analytics and querying.
Analytics: BigQuery is used for querying the Uber data to generate reports and insights.

Flow:
Data Ingestion: Data is ingested from the Uber data source into GCS.
ETL Process: Apache Airflow triggers the ETL tasks:
Extract: Pulls data from GCS.
Transform: The data is cleaned or processed as required.
Load: The transformed data is loaded into BigQuery for storage and analysis.
Data Analysis: BigQuery is used for running SQL queries to analyze the data.

Conclusion
This project demonstrates the ability to process large datasets with GCP's BigQuery and Apache Airflow. It helps in automating data pipelines, enabling efficient analysis and insights from Uber data.
