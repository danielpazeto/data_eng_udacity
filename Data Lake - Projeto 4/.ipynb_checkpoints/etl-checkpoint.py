import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['DEFAULT']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['DEFAULT']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Function which process the song file.
        Read the information from a file and save it on song and artist parquet files in s3
        
        Args
            spark : spark instance
            input_data : root path to input data
            output_data : root path to output data
    """
    
    # get filepath to song data file
    song_data = "{}/song_data/*/*/*/*.json".format(input_data)

    # read song data file
    df = spark.read.json(song_data) 
    
    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'] 
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
        Function which process the log data file.
        Read the information from a file and save it on user, songplays and time(year, month) parquet files in s3
        
        Args
            spark : spark instance
            input_data : root path to input data
            output_data : root path to output data
        
    """
    # get filepath to log data file
    log_data = "{}/log_data/*/*/*/*.json".format(input_data)

    # read log data file
    df = spark.read.json(log_data) 

    # filter by actions for song plays
    songplays_table = df['ts', 'userId', 'level','sessionId', 'location', 'userAgent']

    # extract columns for users table 
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    
    # write users table to parquet files (overwriting case there already a file like that)
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table using the pyspark functions
    # reference from https://sparkbyexamples.com/spark/spark-sql-date-and-time-functions/#year
    time_table = df.select(
        col('datetime').alias('start_time'),
        year('datetime').alias('year'),
        month('datetime').alias('month'),
        weekofyear('datetime').alias('week'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day')
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
        .parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs.parquet')) 

    # extract columns from joined song and log datasets to create songplays table 
    df = df.join(song_df, song_df.title == df.song)
    songplays_table = df['ts', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']
    songplays_table.select(monotonically_increasing_id().alias('songplay_id')).collect()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')


def main():
    """
        Main function which will be called when run `python etl.py`         
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://pazeto-bucket-nd"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
