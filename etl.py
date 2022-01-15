"""
Student: Jonathan Cen
Course: Udacity Data Engineering Nano Degree
Project: Data Lake

s3 URI: https://s3.console.aws.amazon.com/s3/buckets/udacity-dend?region=us-west-2&tab=objects
"""

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, LongType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

# create a schema for song_data to ensure the columns have the correct data types
song_data_schema = StructType([
    StructField(name="artist_id", dataType=StringType(), nullable=True),
    StructField(name="artist_latitude", dataType=DoubleType(), nullable=True),
    StructField(name="artist_location", dataType=StringType(), nullable=True),
    StructField(name="artist_longitude", dataType=DoubleType(), nullable=True),
    StructField(name="artist_name", dataType=StringType(), nullable=True),
    StructField(name="duration", dataType=DoubleType(), nullable=True),
    StructField(name="num_songs", dataType=LongType(), nullable=True),
    StructField(name="song_id", dataType=StringType(), nullable=True),
    StructField(name="title", dataType=StringType(), nullable=True),
    StructField(name="year", dataType=IntegerType(), nullable=True)
])

# create a schema for log_data to ensure the columns have the correct data types
log_data_schema = StructType([
    StructField(name="artist", dataType=StringType(), nullable=True),
    StructField(name="auth", dataType=StringType(), nullable=True),
    StructField(name="firstName", dataType=StringType(), nullable=True),
    StructField(name="gender", dataType=StringType(), nullable=True),
    StructField(name="itemInSession", dataType=LongType(), nullable=True),
    StructField(name="lastName", dataType=StringType(), nullable=True),
    StructField(name="length", dataType=DoubleType(), nullable=True),
    StructField(name="level", dataType=StringType(), nullable=True),
    StructField(name="location", dataType=StringType(), nullable=True),
    StructField(name="method", dataType=StringType(), nullable=True),
    StructField(name="page", dataType=StringType(), nullable=True),
    StructField(name="registration", dataType=DoubleType(), nullable=True),
    StructField(name="sessionId", dataType=LongType(), nullable=True),
    StructField(name="song", dataType=StringType(), nullable=True),
    StructField(name="status", dataType=IntegerType(), nullable=True),
    StructField(name="ts", dataType=LongType(), nullable=True),
    StructField(name="userAgent", dataType=StringType(), nullable=True),
    StructField(name="userId", dataType=StringType(), nullable=True)
])


def create_spark_session():
    """ This function creates a spark session """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ This function reads song data from s3, process them into two dimension tables: songs table and artists table, and finally save them on s3 with the appropriate partitions
    :param spark: a spark session
    :param input_data: the path to the input data
    :param output_data: the path to the output data
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(path=song_data, schema=song_data_schema)

    # extract columns to create songs table
    songs_table = df.select(col('song_id'), col('title'), col('artist_id'), col('year'),
                            col('duration')).distinct()

    # drop any duplicates in songs_table
    songs_table = songs_table.dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and then by artist
    songs_table.write.partitionBy("year", 'artist_id').mode('overwrite').parquet(path=os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df.select(col('artist_id'), col('artist_name').alias('name'),
                              col('artist_location').alias('location'), col('artist_latitude').alias('latitude'),
                              col('artist_longitude').alias('longitude')).distinct()

    # drop any duplicates in artists_table
    artists_table = artists_table.dropDuplicates(['artist_id'])

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(path=os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    """ This function reads log data from s3, process them into three dimension tables: user table, time table, and songplays table, and finally save them on s3 with the appropriate partitions
        :param spark: a spark session
        :param input_data: the path to the input data
        :param output_data: the path to the output data
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')  # for s3
    # log_data = os.path.join(input_data, 'log-data/*.json')  # for local

    # read log data file
    df = spark.read.json(path=log_data, schema=log_data_schema)  # 8056

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')  # 6820

    # extract columns for users table
    users_table = df.select(col('userId').alias('user_id'), col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'), col('gender'), col('level')).distinct()  # 104

    # drop any duplicates in users_table
    users_table = users_table.dropDuplicates(['user_id'])

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(path=os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda timestamp: datetime.fromtimestamp(timestamp / 1000).isoformat())
    df = df.withColumn(colName='start_time', col=get_timestamp('ts').cast(TimestampType()))

    # extract columns to create time table
    time_table = df.select('start_time')
    # extract hour from timestamp
    time_table = time_table.withColumn(colName='hour', col=hour('start_time'))
    # extract day from timestamp
    time_table = time_table.withColumn(colName='day', col=dayofmonth('start_time'))
    # extract week from timestamp
    time_table = time_table.withColumn(colName='week', col=weekofyear('start_time'))
    # extract month from timestamp
    time_table = time_table.withColumn(colName='month', col=month('start_time'))
    # extract year from timestamp
    time_table = time_table.withColumn(colName='year', col=year('start_time'))
    # extract weekday from timestamp
    time_table = time_table.withColumn(colName='weekday', col=dayofweek('start_time'))

    # drop any duplicates in time_table
    time_table = time_table.dropDuplicates(['start_time'])

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode('overwrite').parquet(path=os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_df = spark.read.json(path=os.path.join(input_data, 'song_data/*/*/*/*.json'), schema=song_data_schema)

    song_log_table = df.join(
        other=song_df,
        on=[
            song_df.title == df.song,
            song_df.artist_name == df.artist,
            song_df.duration == df.length
        ],
        how='left'
    )

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = song_log_table.select(col('start_time'), col('userId').alias('user_id'), col('level'),
                                            col('song_id'), col('artist_id'), col('sessionId').alias('session_id'),
                                            col('location'), col('userAgent').alias('user_agent'))

    # create songplay_id
    songplays_table = songplays_table.withColumn(colName="songplay_id", col=monotonically_increasing_id())

    # reorder columns
    songplays_table = songplays_table.select([songplays_table.schema.names[-1]] + songplays_table.schema.names[:-1])

    # create column "month" and "year" for partition purposes
    songplays_table = songplays_table.withColumn(colName="year", col=year(songplays_table.start_time))
    songplays_table = songplays_table.withColumn(colName="month", col=month(songplays_table.start_time))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month") \
        .mode('overwrite').parquet(path=os.path.join(output_data, 'songplays'))


def main():
    """
    This function control the main logic of the etl process.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    # input_data = "data"
    output_data = "s3a://udacity-datalake-results"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
