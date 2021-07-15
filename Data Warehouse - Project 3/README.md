## Sparkify ETL

### This ETL project extract, transform and load the data from files to Redshift.

We have two folder in s3, each one of them files containing json raw data about song and events for the Sparkify music company

###**The raw data files:**
1. Folder **log_data** contain files with the events about each song and user interaction such as example:
`{
	"artist": null,
	"auth": "Logged In",
	"firstName": "Walter",
	"gender": "M",
	"itemInSession": 0,
	"lastName": "Frye",
	"length": null,
	"level": "free",
	"location": "San Francisco-Oakland-Hayward, CA",
	"method": "GET",
	"page": "Home",
	"registration": 1540919166796.0,
	"sessionId": 38,
	"song": null,
	"status": 200,
	"ts": 1541105830796,
	"userAgent": "\\Mozilla\\ / 5.0(Macintosh; Intel Mac OS X 10 _9_4) AppleWebKit\\ / 537.36(KHTML, like Gecko) Chrome\\ / 36.0 .1985 .143 Safari\\ / 537.36\\ ",
	"userId": "39"
}`


1. Folder **log_data** contain files with the song data as the example:
`{
	"num_songs": 1,
	"artist_id": "ARJIE2Y1187B994AB7",
	"artist_latitude": null,
	"artist_longitude": null,
	"artist_location": "",
	"artist_name": "Line Renaud",
	"song_id": "SOUPIRU12A6D4FA1E1",
	"title": "Der Kleine Dompfaff",
	"duration": 152.92036,
	"year": 0
}`

Using **Redshift COPY command** we will extract the json data from both to staging tables(staging_events and staging_songs)

### Files in repository:
1. **dwh.cfg** ->  Contain all the configuration about the raw data files location, Redshift host and credentials
1. **create_tables.py** -> Python file responsible to create all tables in Redshift
1. **sql_queries.py** -> Python files responsible to have all queries(creation and insert) which will run in this process 
1. **etl.py** -> Python file responsible to run the load of staging tables and insertion on the other tables

### How to run the ETL process:
1. First we need to configure the variables in file **dhw.cfg**
1. Now we can create the tables in Redshift database running `python create_tables.py`
1. Then we are able to run the ETL process running `python etl.py`

**After we run the tables should have the following results:**<br>
The users table came from staging_events and we have 104 unique users.<br>
The time table came from staging_events and we have 6813 different times.<br>
The song table came from staging_songs and we have 14896 different songs.<br>
The artist table came from staging_songs and we have 10025 different artist.<br>
The songplaytable came from both staging_events and staging_songs tables and we have 333 played songs<br>


