# Udacity Data Engineering Nanodegree - Data Lake

### Student: Jonathan Cen

## Context

Sparkify is a startup company in the music streaming application business. Sparkify collects song
data and user activities on their app. Sparkify has grown their user base and song database
significantly and want to move their processes and data to a data lake. Their data resides in s3 in
the format of json.

Jonathan Cen is a data engineer who is tasked to help Sparkify to construct a data pipeline to
extract data from various JSON files located on AWS S3, process them with Apache Spark, and finally
load the data back to s3 as a set of 5 dimensional tables in the form of parquet files with
appropriate partitions. With proper design of database schemas and ETL pipelines, the
<code>sparkify</code> data warehouse will support Sparkify's analytics team to perform large scale
analysis on the data they've collected.

## Repository Structure

-   <code>etl.py</code> - this python file contains functions to perform ETL process on all log_data
    files and song_data files.
-   <code>dl.cfg</code> - this config file contains aws credentials to connect to s3
-   <code>README.md</code> - this is the readme file
-   <code>data</code> - this showcase the actual raw data stored on the
    <code>s3a://udacity-dend/</code> bucket
-   <code>results</code> - this showcase how the data would look like in the data lake
    <code>s3a://udacity-datalake-results</code>

## How to run?

Jonathan submits the project on the Udacity Project Workspace. Please follow the following steps on
the Udacity Project Workspace to run the project:

1. Launch the terminal
2. Make sure the directory is <code>/home/workspace</code>
3. Execute the command <code>python etl.py</code>

## Database Schema Design

Since this database is for analytical purposes, it should be optimized for <strong>Online Analytical
Processing (OLAP)</strong> workloads, and therefore, a <strong>STAR schema</strong> is recommended
for this database. After visualising the data Sparkify has been collected and understanding the goal
of the Sparkify analytics team, Jonathan utilizes the data modelling skills he learned in the
Udacity Data Engineering nanodegree program and designs the following database schema for the
<code>sparkify</code>:

-   Table <code>songplays</code> - this table records in log data associated with song plays and it
    is the <strong>fact table</strong> of the STAR schema.
    -   <i>songplay_id</i> - this is the primary key for the <code>songplays</code> table. It
        uniquely identifies each record in the <code>songplays</code> table. Since this table
        records log data, it's primary key <i>songplay_id</i> can be of the data type
        <code>SERIAL</code> and should be automatically incremented as new data arrives in this
        table.
    -   <i>start_time</i> - this is the time when the log is recorded, and it should not be null.
        Data Type = <code>TIMESTAMP</code>
    -   <i>user_id</i> - this is the user id in a particular record, it should be a <strong>foreign
        key</strong> that refers to the <code>users</code> table. Data Type = <code>VARCHAR</code>
    -   <i>level</i> - this field outlines the user's subscription status (e.g. free or paid) to the
        music streaming app. Data Type = <code>VARCHAR</code>
    -   <i>song_id</i> - this is the song id in a particular record, and it should be a
        <strong>foreign key</strong> that refers to the <code>songs</code> table. Data Type =
        <code>VARCHAR</code>
    -   <i>artist_id</i> - this is the artist id for the corresponding song, and it should be a
        <strong>foreign key</strong> that refers to the <code>artists</code> table. Data Type =
        <code>VARCHAR</code>
    -   <i>session_id</i> - this is the session id for a log data record. This should not be NULL.
        Data Type = <code>int</code>
    -   <i>location</i> - this is the geographic location of the user. Data Type =
        <code>VARCHAR</code>
    -   <i>user_agent</i> - this is device the user used to access the music streaming app and it
        can be very long. A data type that can contain lots of text is recommended. Data Type =
        <code>TEXT</code>
-   Table <code>users</code> - this table records the users in the app, and it is one of the
    <strong>dimension tables</strong> of the STAR schema.
    -   <i>user_id</i> - this is the primary key for the <code>users</code> table, and it uniquely
        identifies each record in the table. Data Type = <code>VARCHAR</code>
    -   <i>first_name</i> - this is the first name of a user. Data Type = <code>VARCHAR</code>
    -   <i>last_name</i> - this is the last name of a user. Data Type = <code>VARCHAR</code>
    -   <i>gender</i> - this indicates the gender of a user. To be inclusive, data type
        <code>VARCHAR</code> is used to allow for more than just <i>male</i> or <i>female</i> as
        genders. Data Type = <code>VARCHAR</code>
    -   <i>level</i> - - this field outlines the user's subscription status (e.g. free or paid) to
        the music streaming app. Data Type = <code>VARCHAR</code>
-   Table <code>songs</code> - this table records songs in the music database, and it is one of the
    <strong>dimension tables</strong> of the STAR schema.
    -   <i>song_id</i> - this is the primary key for the <code>songs</code> table, and it uniquely
        identifies each record in the table. Data Type = <code>VARCHAR</code>
    -   <i>title</i> - this is the song title. Data Type = <code>VARCHAR</code>
    -   <i>artist_id</i> - this is the artist id of the song. Data Type = <code>VARCHAR</code>
    -   <i>year</i> - this is the year when the song is release. Data Type = <code>int</code>
    -   <i>duration</i> - this is the duration of a song in the form of seconds. Data Type =
        <code>numeric</code>
-   Table <code>artists</code> - this table records all artists' information in the music database,
    and it is one of the <strong>dimension tables</strong> of the STAR schema.
    -   <i>artist_id</i> - this is the primary key for the <code>artists</code> table, and it
        uniquely identifies each record in the table. Data Type = <code>VARCHAR</code>
    -   <i>name</i> - this is the fullname of an artist. Data Type = <code>VARCHAR</code>
    -   <i>location</i> - this is the location of an artist. Data Type = <code>VARCHAR</code>
    -   <i>latitude</i> - this is the latitude of the artist's location. Data Type =
        <code>numeric</code>
    -   <i>longitude</i> - this is the longitude of the artist's location. Data Type =
        <code>numeric</code>
-   Table <code>time</code> - this table records timestamps of records in <code>songplays</code>
    table broken down into specific units, and it is one of the <strong>dimension tables</strong> of
    the STAR schema.
    -   <i>start_time</i> - this is the primary key for the <code>time</code> table. With the
        precision down to milliseconds, it uniquely identifies each record in the table. Data Type =
        <code>TIMESTAMP</code>
    -   <i>hour</i> - this is the hour section of the timestamp. Data type = <code>int</code>
    -   <i>day</i> - this is the day section of the timestamp. Data type = <code>int</code>
    -   <i>week</i> - this is the week number in a year that of the timestamp. Data type =
        <code>int</code>
    -   <i>month</i> - this is the month section of the timestamp. Data type = <code>int</code>
    -   <i>year</i> - this is the year section of the timestamp. Data type = <code>int</code>
    -   <i>weekday</i> - this is the weekday section of the timestamp. Data type = <code>int</code>

## ETL pipeline design

The ETL pipeline extract data from all JSON file located in the <code>s3a://udacity-dend/</code>
directory one by one, using <code>pyspark</code> to transform the data.

Please see the pseudocode for logic demonstration:

<pre><code>
ETL process:
    Load all song_data files into a single spark dataframe:
        create songs table dataframe
        save the songs tables on s3a://udacity-datalake-results in parquet format
        create artists table
        partition the artists dataframe by year and then by artist_id, finally save the songs tables on s3a://udacity-datalake-results in parquet format
    End of processing song_data files

    Load all log_data files into a single spark dataframe:
        filter the data by 'page' == 'NextSong'
        create users table dataframe
        save the users tables on s3a://udacity-datalake-results in parquet format
        
        create time table dataframe
        partition the time table dataframe by year and then by month, finally save the songs tables on s3a://udacity-datalake-results in parquet format
        
        read in song_data dataframe
        perform a join between the log_data dataframe and the song_data dataframe
        select the required attribute
        insert all records into the songplays table
    End of processing log_data files
End of ETL process
</code></pre>
