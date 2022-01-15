"""
etl.py
Course: Udacity Data Engineering Program
Student: Jonathan Cen
"""


import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Process one song_data file as provded by the filepath variable
    - Extracts and Transform any relevant song data from the song_data file and insert it to the songs table
    - Extracts and Transform any relevant artist data from the song_data file and insert it to the artists table
    - Return: None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)   # lines=True because the source file is line-separated json.

    # insert song record (song_id, title, artist_id, year, duration)
    song_data = [df["song_id"].values[0], df["title"].values[0], df["artist_id"].values[0], int(df["year"].values[0]), float(df["duration"].values[0])]
    # replace nan value with None so that it is inserted as Null in PostgreSQL
    song_data = [value if pd.notna(value) else None for value in song_data]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record (artist_id, name, location, latitude, longitude)
    artist_data = [df["artist_id"].values[0], df["artist_name"].values[0], df["artist_location"].values[0], df["artist_latitude"].values[0], df["artist_longitude"].values[0]]
    # replace nan value with None so that it is inserted as Null in PostgreSQL
    artist_data = [value if pd.notna(value) else None for value in artist_data]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Process one log_data file as provded by the filepath variable
    - Extracts and Transform any relevant time data from the log_data file and insert it to the time table
    - Extracts and Transform any relevant user data from the log_data file and insert it to the users table
    - Perform a query on both the songs and artists tables to obtain songid and artistid, combining with relevant data from log_data file, and insert it to the songplays table
    - Return: None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit='ms')
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(dict((column_labels[i], time_data[i]) for i in range(len(column_labels))))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    column_labels = ("user_id", "first_name", "last_name", "gender", "level")
    user_data = [df["userId"], df["firstName"], df["lastName"], df["gender"], df["level"]]
    user_df = pd.DataFrame(dict((column_labels[i], user_data[i]) for i in range(len(column_labels))))

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid,
                         artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - A master function that retrives all data files in the provided filepath. 
    - For each data file, this function then calls the appropriate ETL fucntion to ETL the data and insert into the appropriate table.
    - Return None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):  # passing one file at a time to func
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Make a connection to the sparkify database using the student credentials
    - Make a cursor
    - ETL for song_data
    - ETL for log_data
    - Finally close the connection
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()