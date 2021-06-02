#ETl for Sparkify played songs

We have data divided in two main folders, log_data and song_data.
In song data we have the main information about the song and artists, while in the log_data files we have the information about the user, the session in the Sparkfy app and other

Our ETL will get each file, extract those info, do a basic transformation and load it in a relational database

In our relation database we will have 4 dimesion tables (time, users, songs and artist) and 1 fact table (songplays)

In order to run the ETL process we need first create the tables in database. It will drop all of them if they already exists
1. Run: python create_tables.py 

Now you can run the ETL procress
2. Run: python etl.py


In this repo you will find some other files

etl.ipynb: notebook which was used to implement the initial version and make test for the etl process
sql_queries.py: this python file contains all queries(CRUD) used in the process
test.ipynb: this notebook was used to as a test one, just to make sure that the items were inserted in the database
data: folder containing the files which was used on the etl process

