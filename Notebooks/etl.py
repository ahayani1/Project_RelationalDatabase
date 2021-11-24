import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

#ignore Setting with copy warning in Pandas / for line 46
pd.options.mode.chained_assignment = None  # default='warn'


def process_song_file(cur, filepath):
    """
    Description: This function is responsible for processing the data related to the songs datasource
    which is specified in the filepath variable. The data is inserted into the tables.

    Arguments:
        cur: the cursor object.
        filepath: song data file path.

    Returns:
        None
    """
    # open song file
    
    df_songs_raw = pd.read_json(filepath, lines=True)

    # insert song record
    df_songs = df_songs_raw[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = list(df_songs.values[0])

    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    df_artists = df_songs_raw[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = list(df_artists.values[0])
    
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: This function is responsible for processing the data related to the logs datasource
    which is specified in the filepath variable. The data is inserted into the tables.

    Arguments:
        cur: the cursor object.
        filepath: log data file path.

    Returns:
        None
    """
    
    # open log file
    
    df_logs = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df_logs_next_song = df_logs.loc[df_logs['page'] == 'NextSong']
    df_logs_next_song.head()

    # convert timestamp column to datetime
    df_logs_next_song['ts'] = pd.to_datetime(df_logs_next_song['ts'], unit = 'ms')
    t = df_logs_next_song['ts']
    
    #create time data df from log file using zip function and timestamp column above
    
    time_data = list(zip(t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday))
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df_raw = pd.DataFrame(time_data, columns = column_labels)
    
    
    #drop na values in time_df_raw
    time_df = time_df_raw.dropna()
    
    #assign weekday names to weekday column
    
    days = {0: 'Mon', 1:'Tues', 2:'Weds', 3:'Thurs', 4:'Fri', 5: 'Sat', 6: 'Sun'}
    time_df['weekday'] = time_df['weekday'].apply(lambda x: days[x])
    
    
    #insert time data records
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df_logs[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df_logs.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit = 'ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function is responsible for listing the files in a directory,
    and then executing the ingest process for each file according to the function
    that performs the transformation to save it to the database.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data or song data file path.
        func: function that transforms the data and inserts it into the database.

    Returns:
        None
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
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()