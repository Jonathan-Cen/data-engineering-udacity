class SqlQueries:

    # --- Drop Table Queries --- #
    staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
    staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
    songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
    user_table_drop = "DROP TABLE IF EXISTS users;"
    song_table_drop = "DROP TABLE IF EXISTS songs;"
    artist_table_drop = "DROP TABLE IF EXISTS artists;"
    time_table_drop = "DROP TABLE IF EXISTS time;"

    # --- Create Table Queries --- #
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

    # --- Insert Table Queries --- #
    songplay_table_insert = ("""
        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT
                events.start_time, 
                events.user_id, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.session_id, 
                events.location, 
                events.user_agent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

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