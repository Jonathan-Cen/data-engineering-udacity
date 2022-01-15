"""
sql_queries.py

Student: Jonathan Cen
Description: this script contains necessary SQL statements will be used in this project.


CREATE TABLE on Redshift is different from on PostgreSQL, see the link below for more info:
    https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html
"""

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSON_PATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')



# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events 
    (
        staging_event_id INTEGER IDENTITY(0, 1) NOT NULL PRIMARY KEY,
        artist VARCHAR(MAX) NULL,
        auth VARCHAR(MAX) NULL,
        first_name VARCHAR(MAX) NULL,
        gender VARCHAR(MAX) NULL,
        item_in_session INTEGER NULL,
        last_name VARCHAR(MAX) NULL,
        length NUMERIC NULL,
        level VARCHAR(MAX) NULL,
        location VARCHAR(MAX) NULL,
        method VARCHAR(MAX) NULL,
        page VARCHAR(MAX) NULL,
        registration NUMERIC NULL,
        session_id INTEGER NULL,
        song VARCHAR(MAX) NULL,
        status INTEGER NULL,
        ts BIGINT NULL,
        user_agent VARCHAR(MAX) NULL,
        user_id VARCHAR(MAX) NULL
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        staging_song_id INTEGER IDENTITY(0, 1) NOT NULL PRIMARY KEY,
        artist_id VARCHAR(MAX) NULL,
        artist_latitude NUMERIC NULL,
        artist_location VARCHAR(MAX) NULL,
        artist_longitude NUMERIC NULL,
        artist_name VARCHAR(MAX) NULL,
        duration NUMERIC NULL,
        num_songs INTEGER NULL,
        song_id VARCHAR(MAX) NULL,
        title VARCHAR(MAX) NULL,
        year INTEGER NULL
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays 
    (
        songplay_id INTEGER IDENTITY(0, 1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id VARCHAR(MAX) NOT NULL,
        level VARCHAR(MAX),
        song_id VARCHAR(MAX) NULL,
        artist_id VARCHAR(MAX) NULL,
        session_id INTEGER NOT NULL,
        location VARCHAR(MAX),
        user_agent TEXT
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users 
    (
        user_id VARCHAR(MAX) PRIMARY KEY,
        first_name VARCHAR(MAX),
        last_name VARCHAR(MAX),
        gender VARCHAR(MAX), 
        level VARCHAR(MAX)
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs 
    (
        song_id VARCHAR(MAX) PRIMARY KEY,
        title VARCHAR(MAX),
        artist_id VARCHAR(MAX),
        year INTEGER,
        duration NUMERIC
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists 
    (
        artist_id VARCHAR(MAX) PRIMARY KEY,
        name VARCHAR(MAX),
        location VARCHAR(MAX),
        latitude NUMERIC,
        longitude NUMERIC
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time 
    (
        start_time TIMESTAMP PRIMARY KEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json {}
""").format(LOG_DATA, IAM_ROLE_ARN, LOG_JSON_PATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 'auto'
""").format(SONG_DATA, IAM_ROLE_ARN)

# FINAL TABLES
# SQL to SQL ETL

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM
        staging_events
    WHERE 
        page = 'NextSong' AND
        user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM
        staging_songs    
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    FROM
        staging_songs
    WHERE
        artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
        EXTRACT(hour FROM TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second') AS hour,
        EXTRACT(day FROM TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second') as day,
        EXTRACT(week FROM TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second') as week,
        EXTRACT(month FROM TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second') as month,
        EXTRACT(year FROM TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second') as year,
        EXTRACT(DOW FROM TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second') as weekday
    FROM
        staging_events se
    WHERE 
        page = 'NextSong' AND 
        start_time NOT IN (SELECT DISTINCT start_time FROM time)
""")

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + staging_events.ts/1000 * interval '1 second' AS start_time,
        staging_events.user_id AS user_id,
        staging_events.level AS level,
        staging_songs.song_id AS song_id,
        staging_songs.artist_id AS artist_id,
        staging_events.session_id AS session_id,
        staging_events.location AS location,
        staging_events.user_agent AS user_agent
    FROM 
        staging_events JOIN staging_songs ON 
            (
                staging_events.song = staging_songs.title AND 
                staging_events.artist = staging_songs.artist_name AND
                staging_events.length = staging_songs.duration
            )

    WHERE 
        staging_events.page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
