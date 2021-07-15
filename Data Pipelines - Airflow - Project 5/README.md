## ETL Overview
Airflow ETL to get data from s3 , transform and load it to Redshift

## Data in s3
1. Log data: s3://udacity-dend/log_data
About the events from users
2. Song data: s3://udacity-dend/song_data
FIles with the song data


### Pre requirements
Create tables in Redshift using the script: create_tables.sql

### Files you can found on the repo
create_tables.sql - Contains the DDL for all tables used in this projecs
udac_example_dag.py - The DAG configuration file to run in Airflow
stage_redshift.py - Airflow Operator to read files from S3 and load into Redshift staging tables
load_fact.py - Airflow Operator to load the fact table in Redshift
load_dimension.py - Airflow  Operator to read from staging tables and load the dimension tables in Redshift
data_quality.py - Airflow Operator for data quality checking