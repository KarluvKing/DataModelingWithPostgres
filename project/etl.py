import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

"""
This procedure processes a song file whose filepath has been provided as an arugment.
It extracts the song information in order to store it into the songs table.
Then it extracts the artist information in order to store it into the artists table.

INPUTS:
* cur the cursor variable
* filepath the file path to the song file
"""

def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record

    song_data = []
    song_data.append(df.values[0][7]) #song_id
    song_data.append(df.values[0][8]) #title
    song_data.append(df.values[0][0]) #artist_id
    song_data.append(df.values[0][9]) #year
    song_data.append(df.values[0][5]) #duration

    cur.execute(song_table_insert, song_data)

    # insert artist record

    artist_data = []
    artist_data.append(df.values[0][0]) #artist_id
    artist_data.append(df.values[0][4]) #name
    artist_data.append(df.values[0][2]) #location
    artist_data.append(df.values[0][1]) #latitude
    artist_data.append(df.values[0][3]) #longitude

    cur.execute(artist_table_insert, artist_data)

"""
This procedure processes a log file whose filepath has been provided as an arugment.
It extracts time data records information in order to store it into the time table.
It also extracts user data record information in order to store it into the user table.
In the end extracts song and artists record information in order to store it into songplay table.

INPUTS:
* cur the cursor variable
* filepath the file path to the log file
"""

def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.query("page == 'NextSong'")

    # convert timestamp column to datetime
    t = df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday
    column_labels = ("Timestamp","Hour","Day","Week of the Year","Month","Year","Weekday")

    time_df = {}
    for i,j in zip(time_data,column_labels):
        time_df[j] = i
    time_df = pd.DataFrame.from_dict(time_df)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:, ["userId","firstName","lastName","gender","level"]]

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
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

"""
This procedure processes all json files whose filepath has been provided as an arugment.
Processes the data from json files whose filepath has been provided as an argument.
Calls the functions process_song_file and process_log_file to extract the necessary data.

INPUTS:
* cur the cursor variable
* filepath the file path to the song file
* conn database connection atributes
* func the function that will process the file
"""

def process_data(cur, conn, filepath, func):
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
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

"""
It creates the connection with the database and calls the functions to process the files.

INPUTS:
* No inputs
"""

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
