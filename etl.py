import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    dfsong = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = dfsong.select(dfsong['song_id'], dfsong['title'], dfsong['artist_id'], dfsong['year'], dfsong['duration']).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist").mode("overwrite").parquet(os.path.join(output_data, 'songs_table/songs.parquet'))

    # extract columns to create artists table
    artists_table = dfsong.select(dfsong['artist_id'], dfsong['artist_name'].alias('name'), dfsong['artist_location'].alias('location')\
                                  , dfsong['artist_latitude'].alias('latitude'), dfsong['artist_longitude'].alias('longitude')).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists_table/artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    dflog = spark.read.json(log_data)
    
    # filter by actions for song plays
    dflog = dflog.where(dflog.page == 'NextSong')

    # extract columns for users table    
    users_table = dflog.select(dflog['userId'], dflog['first_name'], dflog['last_name'], dflog['gender'], dflog['level']).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users_table/users.parquet'), 'overwrite')
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: F.to_timestamp(x / 1000.0))
    dflog = dfLog.withColumn("start_time", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    time_table = dflog.select('start_time')
    time_table = time_table.withColumn('hour', F.hour('start_time'))
    time_table = time_table.withColumn('day', F.dayofmonth('start_time'))
    time_table = time_table.withColumn('week', F.weekofyear('start_time'))
    time_table = time_table.withColumn('month', F.month('start_time'))
    time_table = time_table.withColumn('year', F.year('start_time'))
    time_table = time_table.withColumn('weekday', F.dayofweek('start_time'))
    
    # extract columns to create time table
    time_table = dflog.select(dflog['start_time'], dflog['hour'], dflog['day'], dflog['week'], dflog['month'],dflog['year'],\
                              dflog['weekday']).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.write.parquet(os.path.join(output_data, 'time_table/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.withColumn('songplay_id', F.monotonically_increasing_id())
    songplays_table = dflog.join(song_df, dflog.artist == song_df.artist_name, "inner").\
                    select(dflog['start_time'], dflog['userId'], dflog['level'], song_df['song_id'], song_df['artist_id'], dflog['sessionId'],dflog['location'],dflog['userAgent']).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(os.path.join(output_data, 'songplay_table/songplay.parquet'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://nadata-lake-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
