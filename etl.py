import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Processes a JSON file containing song information given a database
    connection cursor and the file's path.
    
    Inputs:
        - cur: a psycopg2 database connection cursor
        - filepath (str): path to the file to be processed
    
    Outputs:
        None
    
    Actions:
        Inserts the data from the JSON file into the songs and arstists
        tables.
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Processes a JSON file containing log information given a database
    connection cursor and the file's path.
    
    Inputs:
        - cur: a psycopg2 database connection cursor
        - filepath (str): path to the file to be processed
    
    Outputs:
        None
    
    Actions:
        Inserts the data from the JSON file into the time, users, and
        songplays tables.
    '''
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records
    time_data = {
    'start_time': t,
    'session_id': df.sessionId,
    'hour': t.dt.hour,
    'day': t.dt.day,
    'week': t.dt.week,
    'month': t.dt.month,
    'year': t.dt.year,
    'weekday': t.dt.weekday
    }
    
    column_labels = (
        'start_time',
        'session_id',
        'hour',
        'day',
        'week',
        'month',
        'year',
        'weekday'
    )
    
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

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
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Processes all JSON files in a folder containing either song or log
    information given a database connection and cursor, the folder's
    path, and the function determining if the file should be processed
    as song data or log data.
    
    Inputs:
        - cur: a psycopg2 database connection cursor
        - conn: a psycopg2 database connection
        - filepath (str): path to the folder to be processed
        - func (function): either ``process_log_file`` or
          ``process_log_file`` 
    
    Outputs:
        None
    
    Actions:
        Inserts the data from the folder's JSON files into the
        database.
    '''
    
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


def main():
    '''
    Main driver function executing the script's functionality.
    It populates the database
    '''
    
    # Execute the ``create_tables`` script:
    with open('./create_tables.py', 'rb') as source_file:
        code = compile(source_file.read(), './create_tables.py', 'exec')
        exec(code, {'__name__': '__main__'})
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()