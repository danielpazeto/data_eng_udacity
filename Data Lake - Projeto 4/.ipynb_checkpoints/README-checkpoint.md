# ETL for Sparkyfi company data
This project is an ETL to extract data located in s3, tranform it and load again to s3 using Spark

1. The Data
We have the data divided in two main folders, log_data and song_data.
In song data we have the main information about the song and artists, while in the log_data files we have the information about the user, the session in the Sparkfy app and other

2. The ETL process
Our ETL will get each file, extract those info, do a basic transformation and load it in amazon s3


3. The transformed data
In our final data we will have 4 dimesion tables (time, users, songs and artist) and 1 fact table (songplays)


### In this repo you will find some other files
dl.cfg - Config file to insert the AWS Credentials
etl.py - The ETL which process data using Spark
notebook.ypynb - file to perform tests and drafts before run the etl.py as a whole

# How to run
Just run it on the terminal
`python etl.py`