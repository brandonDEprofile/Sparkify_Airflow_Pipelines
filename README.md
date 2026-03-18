Sparkify Airflow Data Pipelines
Overview

This project builds an end-to-end data pipeline using Apache Airflow to orchestrate ETL processes that load data from Amazon S3 into Amazon Redshift Serverless. The pipeline stages raw JSON data, transforms it into a star schema, and performs data quality checks.

The project simulates a music streaming company (Sparkify) that needs automated and reliable data pipelines for analytics.

Architecture

Source:

S3 (JSON logs + song metadata)

Processing:

Apache Airflow (custom operators)

Destination:

Amazon Redshift Serverless (data warehouse)

DAG Workflow
Begin_execution
        ↓
Stage_events      Stage_songs
        ↓              ↓
      Load_songplays_fact_table
                ↓
   ---------------------------------------
   ↓        ↓        ↓        ↓
Users    Songs    Artists    Time
   ---------------------------------------
                ↓
     Run_data_quality_checks
                ↓
           Stop_execution
Features

Dynamic and reusable Airflow tasks

Custom operators for:

Staging data from S3

Loading fact tables

Loading dimension tables

Data quality checks

Parameterized SQL execution

Automated retries and scheduling

Hourly pipeline execution

Scalable Redshift data warehouse integration

Project Structure
dags/
    sparkify_dag.py

plugins/
    operators/
        stage_redshift.py
        load_fact.py
        load_dimension.py
        data_quality.py

    helpers/
        sql_queries.py
Data Sources

Log Data: s3://udacity-dend/log_data

Song Data: s3://udacity-dend/song-data

Copied to:

s3://brand-sparkify-airflow-2026/
Tables
Fact Table

songplays

Dimension Tables

users

songs

artists

time

Setup Instructions
1. AWS Setup

Create IAM user with programmatic access

Create S3 bucket

Configure Redshift Serverless

Attach IAM roles with S3 access

2. Airflow Setup (Local - WSL)
python -m venv .venv
source .venv/bin/activate
pip install apache-airflow

Run Airflow:

export AIRFLOW_HOME=~/airflow-project/airflow-home
export PYTHONPATH=~/airflow-project
airflow standalone
3. Airflow Connections
AWS Credentials

Conn ID: aws_credentials

Type: Amazon Web Services

Access Key / Secret Key

Redshift

Conn ID: redshift

Type: Postgres

Host: <your-redshift-endpoint>

Database: dev

User: awsuser

Password: <your-password>

Port: 5439

Data Quality Checks

The pipeline validates:

Tables are not empty

Data is successfully loaded into Redshift

Example check:

SELECT COUNT(*) FROM songplays;
Technologies Used

Python

Apache Airflow

Amazon S3

Amazon Redshift Serverless

PostgreSQL (via Redshift)

AWS IAM

Key Learnings

Building scalable ETL pipelines with Airflow

Creating reusable custom operators

Managing AWS services programmatically

Debugging real-world data pipeline issues

Implementing data quality validation

Author

Brandon

License

This project is for educational purposes.
