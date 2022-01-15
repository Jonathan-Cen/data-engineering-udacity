# Udacity Data Engineering Nanodegree - Data Modeling with Postgres

### Student: Jonathan Cen

## Context

Sparkify is a startup company in the music streaming application business. Sparkify collects song
data and user activities on their app. Sparkify's data analytics team is at stage to perform data
analysis on all the data they collected to understand what songs their users are listening to.
Sparkify's music streaming app stores two types of data, song data and log data, both are in the
form of JSON files.

Jonathan Cen is a data engineer who is tasked to help Sparkify to create a relational database named
sparkifydb on PostgreSQL, construct a data pipeline to extract data from various JSON files,
transform the data, and finally load the data on to the <code>sparkifydb</code>. With proper design
of database schemas and ETL pipelines, the <code>sparkifydb</code> will support Sparkify's analytics
team to perform analysis on the data they've collected.

## Repository Structure

-   <code>create_tables.py</code> - this python file contains functions to drop and create the
    <code>sparkify</code> database and its tables.
-   <code>data</code> - this directory contains all song_data json files and log_data json files
-   <code>etl.ipynb</code> - this IPython notebook shows a complete process to perform ETL on one
    song_data file and one log_data file.
-   <code>etl.py</code> - this python file contains functions to perform ETL process on all log_data
    files and song_data files.
-   <code>sql_queries.py</code> - this python file contains all necessary SQL queries used in this
    project
-   <code>test.ipynb</code> - this IPython notebook contains simple SQL queries for testing ETL
    results on the <code>sparkify</code> database.

## How to run?

Jonathan submits the project on the Udacity Project Workspace. Please follow the following steps on
the Udacity Project Workspace to run the project:

1. Launch the terminal
2. Make sure the directory is <code>/home/workspace</code>
3. Execute the command <code>python create_tables.py</code> to create the <code>sparkifydb</code>
   database and all relevant tables. No output is printed on the console as a result of executing
   this command.
4. Execute the command <code>python etl.py</code> to extract and transform data from various JSON
   files and load the data into the <code>sparkifydb</code> database.
    - Indications of the step 4 executed successfully:
        - There should be 72 files found in the <code>data/song_data</code> directory.
        - <code>1/72 files processed.</code> until <code>72/72 files processed.</code> should be
          printed on the console.
        - There should be 31 files found in the <code>data/log_file</code> directory.
        - <code>1/31 files processed.</code> until <code>31/31 files processed.</code> should be
          printed on the console.
        - No error message shall be encountered.
5. Double click test.ipynb and select <code>Kernel</code> --> <code>Restart Kernel and Run All
   Cells...</code> to verify the success of ETL
    - All SQL command shall be executed without any error.
    - Cell 3, 4, 5, 6, and 7 should all return 5 records.
    - Jonathan added three extra cells to verify "<i>It’s okay if there are some null values for
      song titles and artist names in the songplays table. There is only 1 actual row that will have
      a songid and an artistid.</i>"
        - Cell 8, 9, and 10 should all return 1 record with the following detail:
            - {"start_time": 2018-11-21 21:56:47.796000, "user_id": 15, "level": paid, "song_id":
              SOZCTXZ12AB0182364, "artist_id": AR5KOSW1187FB35FF4, "session_id": 818, "location":
              Chicago-Naperville-Elgin, IL-IN-W, "user_agent": "Mozilla/5.0 (X11; Linux x86_64)
              AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125
              Chrome/36.0.1985.125 Safari/537.36"}
            - <strong>NOTE:</strong> <code>songplay_id</code> may vary depending on when the log
              file is processed, but the remaining data in the record should match what is provided
              at above.

## Database Schema Design

Since this database is for analytical purposes, it should be optimized for <strong>Online Analytical
Processing (OLAP)</strong> workloads, and therefore, a <strong>STAR schema</strong> is recommended
for this database. After visualising the data Sparkify has been collected and understanding the goal
of the Sparkify analytics team, Jonathan utilizes the data modelling skills he learned in the
Udacity Data Engineering nanodegree program and designs the following database schema for the
<code>sparkifydb</code>:

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

The ETL pipeline extract data from all JSON file located in the data directory one by one, using the
Python <code>pandas</code> library to transform the data. A <code>pandas DataFrame</code> is created
for each table in the <code>sparkify</code> database, and finally each record in the
<code>DataFrame</code> is inserted into the database.

Please see the pseudocode for logic demonstration:

<pre><code>
ETL process:
    Find all song_data files and for each song_data json file:
        extract relevant song data from the song_data file in a DataFrame
        apply transformation
        insert each record from the DataFrame into the songs table
    
        extract relevant artist data from the song_data file in a DataFrame
        apply transformation
        insert each record from the DataFrame into the artists table
    End of processing song_data files

    Find all log_data files and for each log_data json file:
        extract relevant timestamp data from the log_data file in a DataFrame
        apply transformation
        insert each record from the DataFrame into the time table
        
        extract relevant user data from the log_data file in a DataFrame
        apply transformation
        insert each record from the DataFrame into the users table

        perform a query on songs table and artists table to get songid and artistid
        apply transformation and get more data from the log_data DataFrame
        insert each record from the DataFrame into the songplays table
    End of processing log_data files
End of ETL process
</code></pre>
