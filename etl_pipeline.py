from airflow import DAG  # type: ignore
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator  # type: ignore
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator  # type: ignore
from airflow.operators.python_operator import PythonOperator  # type: ignore
import pandas as pd
import numpy as np  # type: ignore
import uuid
from datetime import datetime, timedelta

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG
dag = DAG(
    'uber_etl_dag',
    default_args=default_args,
    description='Uber Data ETL to BigQuery',
    schedule_interval='@daily',  # Runs every day
)

# ETL Function
def transform_uber_data(**kwargs):
    url = "https://storage.googleapis.com/uber_data_analytics_prathamesh/uber_data.csv"
    df = pd.read_csv(url)

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    df = df.drop_duplicates().reset_index(drop=True)
    df['trip_ID'] = [str(uuid.uuid4()) for _ in range(len(df))]

    # Dimension Tables
    dim_vendor = df[['VendorID']].drop_duplicates().reset_index(drop=True)
    dim_vendor['vendor_ID'] = dim_vendor['VendorID']
    dim_vendor['vendor_name'] = dim_vendor['VendorID'].map({1: 'Creative Mobile Technologies', 2: 'VeriFone'})
    dim_vendor = dim_vendor[['vendor_ID', 'vendor_name']]

    dim_payment_type = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    dim_payment_type['payment_type_ID'] = dim_payment_type['payment_type']
    dim_payment_type['payment_description'] = dim_payment_type['payment_type'].map({
        1: 'Credit Card', 2: 'Cash', 3: 'No Charge', 4: 'Dispute', 5: 'Unknown', 6: 'Voided Trip'
    })
    dim_payment_type = dim_payment_type[['payment_type_ID', 'payment_description']]

    dim_ratecode = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    dim_ratecode['ratecode_ID'] = dim_ratecode['RatecodeID']
    dim_ratecode['ratecode_description'] = dim_ratecode['RatecodeID'].map({
        1: 'Standard rate', 2: 'JFK', 3: 'Newark', 4: 'Nassau or Westchester',
        5: 'Negotiated fare', 6: 'Group ride'
    })
    dim_ratecode = dim_ratecode[['ratecode_ID', 'ratecode_description']]

    dim_pickup_location = df[['pickup_longitude', 'pickup_latitude']].drop_duplicates().reset_index(drop=True)
    dim_pickup_location['pickup_location_ID'] = dim_pickup_location.index
    dim_pickup_location['zone_name'] = np.random.choice(
        ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island'], size=len(dim_pickup_location)
    )
    dim_pickup_location['borough'] = np.random.choice(
        ['Downtown', 'Midtown', 'Uptown', 'Central'], size=len(dim_pickup_location)
    )
    dim_pickup_location = dim_pickup_location[['pickup_location_ID', 'pickup_longitude', 'pickup_latitude', 'zone_name', 'borough']]

    dim_dropoff_location = df[['dropoff_longitude', 'dropoff_latitude']].drop_duplicates().reset_index(drop=True)
    dim_dropoff_location['dropoff_location_ID'] = dim_dropoff_location.index
    dim_dropoff_location['zone_name'] = np.random.choice(
        ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island'], size=len(dim_dropoff_location)
    )
    dim_dropoff_location['borough'] = np.random.choice(
        ['Downtown', 'Midtown', 'Uptown', 'Central'], size=len(dim_dropoff_location)
    )
    dim_dropoff_location = dim_dropoff_location[['dropoff_location_ID', 'dropoff_longitude', 'dropoff_latitude', 'zone_name', 'borough']]

    # Fact Table
    fact_trip = df[['trip_ID', 'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
                    'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude',
                    'RatecodeID', 'store_and_fwd_flag', 'payment_type', 'fare_amount', 'extra', 'mta_tax',
                    'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount']]

    # Merging DataFrames
    fact_trip = fact_trip.merge(dim_vendor, left_on='VendorID', right_on='vendor_ID', how='left').drop(columns=['VendorID'])
    fact_trip = fact_trip.merge(dim_pickup_location, on=['pickup_longitude', 'pickup_latitude'], how='left').drop(columns=['pickup_longitude', 'pickup_latitude'])
    fact_trip = fact_trip.merge(dim_dropoff_location, on=['dropoff_longitude', 'dropoff_latitude'], how='left').drop(columns=['dropoff_longitude', 'dropoff_latitude'])
    fact_trip = fact_trip.merge(dim_ratecode, left_on='RatecodeID', right_on='ratecode_ID', how='left').drop(columns=['RatecodeID'])
    fact_trip = fact_trip.merge(dim_payment_type, left_on='payment_type', right_on='payment_type_ID', how='left').drop(columns=['payment_type'])

    # Save to CSV
    dim_vendor.to_csv('/tmp/dim_vendor.csv', index=False)
    dim_pickup_location.to_csv('/tmp/dim_pickup_location.csv', index=False)
    dim_dropoff_location.to_csv('/tmp/dim_dropoff_location.csv', index=False)
    dim_ratecode.to_csv('/tmp/dim_ratecode.csv', index=False)
    dim_payment_type.to_csv('/tmp/dim_payment_type.csv', index=False)
    fact_trip.to_csv('/tmp/fact_trip.csv', index=False)

# Tasks
transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_uber_data,
    provide_context=True,
    dag=dag,
)

# Upload to GCS
tables = ["dim_vendor", "dim_pickup_location", "dim_dropoff_location", "dim_ratecode", "dim_payment_type", "fact_trip"]
upload_tasks = [
    LocalFilesystemToGCSOperator(
        task_id=f"upload_{table}",
        src=f"/tmp/{table}.csv",
        dst=f"{table}.csv",
        bucket="uber_data_analytics_prathamesh",
        dag=dag
    ) for table in tables
]

# Load to BigQuery
load_tasks = [
    GCSToBigQueryOperator(
        task_id=f"load_{table}",
        bucket='uber_data_analytics_prathamesh',
        source_objects=[f"{table}.csv"],
        destination_project_dataset_table=f'uberdataanalyticsprathamesh.uber_dataEngineering.{table}',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        field_delimiter=',',
        skip_leading_rows=1,
        dag=dag
    ) for table in tables
]

# Task Dependencies
# Set dependency: transform_data_task must finish before all upload tasks start
transform_data_task >> upload_tasks

# Set dependency: each upload task must complete before its corresponding load task starts
for upload_task, load_task in zip(upload_tasks, load_tasks):
    upload_task >> load_task