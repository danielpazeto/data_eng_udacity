import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events 
(
  artist VARCHAR(256),
  auth VARCHAR(22),
  first_name VARCHAR(256),
  gender VARCHAR(1),
  intem_in_session INTEGER,
  last_name VARCHAR(256),
  length DECIMAL(10,2),
  level VARCHAR(10),
  location VARCHAR(256),
  method VARCHAR(10),
  page VARCHAR(50),
  registration VARCHAR(256),
  session_id INTEGER,
  song VARCHAR(256),
  status INTEGER,
  ts VARCHAR(50),
  user_agent VARCHAR(256),
  user_id INTEGER
);

""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs 
(
  num_songs INTEGER,
  artist_id VARCHAR(50),
  artist_latitude DECIMAL(10,2),
  artist_longitude DECIMAL(10,2),
  artist_location VARCHAR(256),
  artist_name VARCHAR(256),
  song_id VARCHAR(150),
  title VARCHAR(512),
  duration DECIMAL(10,2),
  year INTEGER NOT NULL
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
    songplay_id INTEGER IDENTITY(0,1),
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    level VARCHAR(50),
    song_id VARCHAR(50) NOT NULL,
    artist_id VARCHAR(50) NOT NULL,
    session_id INTEGER,
    location VARCHAR(256),
    user_agent VARCHAR(512),
    PRIMARY KEY(songplay_id)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS "users" (
    user_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(150) NOT NULL,
    last_name VARCHAR(150) NOT NULL,
    gender CHAR NOT NULL, 
    level VARCHAR(50),
    PRIMARY KEY(user_id)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song(
    song_id VARCHAR(50) NOT NULL,
    title VARCHAR(512) NOT NULL, 
    artist_id VARCHAR(256) NOT NULL, 
    year INTEGER NOT NULL,
    duration NUMERIC,
    PRIMARY KEY(song_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist(
    artist_id VARCHAR(50), 
    name VARCHAR(256) NOT NULL,
    location VARCHAR(256),
    latitude NUMERIC,
    longitude NUMERIC,
    PRIMARY KEY(artist_id)
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP NOT NULL,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL, 
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL, 
    weekday text NOT NULL,
    PRIMARY KEY(start_time)
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE {}
REGION 'us-west-2'
FORMAT JSON AS {};
""").format(config['S3']['LOG_DATA'], 
            config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE {}
REGION 'us-west-2'
JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT
    timestamp 'epoch' + cast(evt.ts AS bigint)/1000 * interval '1 second' AS start_time,
    evt.user_id,
    evt.level,
    sg.song_id,
    sg.artist_id,
    evt.session_id,
    evt.location,
    evt.user_agent
FROM staging_events evt
JOIN staging_songs sg ON  evt.artist=sg.artist_name AND evt.song=sg.title
WHERE evt.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO "users"
SELECT DISTINCT
    user_id,
    first_name,
    last_name,
    gender,
    level
FROM staging_events WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO song
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artist
SELECT DISTINCT
    artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time 
WITH timestamps AS(
    SELECT timestamp 'epoch' + cast(ts AS bigint)/1000 * interval '1 second' AS ts 
    FROM staging_events  WHERE page = 'NextSong'
)
SELECT DISTINCT
    ts,
    extract(hour from ts) AS start_time,
    extract(day from ts) AS hour,
    extract(week from ts) AS day,
    extract(month from ts) AS week,
    extract(year from ts) AS month,
    extract(weekday from ts) AS weekday
FROM timestamps
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
