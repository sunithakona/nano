# %%
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    '''Loads the event and songs staging tables from s3 buckets 
       by executing queries in copy_table_queries array'''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''Loads the fact and dimension tables from the staging tables
       by executing queries in insert_table_queries array'''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def printOutput(cur):
    '''Prints output after the load is complete'''
    staging_events_table_select = "SELECT COUNT(*) FROM staging_events;"
    staging_songs_table_select = "SELECT COUNT(*) FROM staging_songs"
    songplay_table_select = "SELECT COUNT(*) FROM songplay"
    user_table_select = "SELECT COUNT(*) FROM users"
    song_table_select = "SELECT COUNT(*) FROM songs"
    artist_table_select = "SELECT COUNT(*) FROM artists"
    time_table_select = "SELECT COUNT(*) FROM time" 
    select_table_queries =[staging_events_table_select, staging_songs_table_select, songplay_table_select, user_table_select, song_table_select, artist_table_select, time_table_select]
    for query in select_table_queries:
        print(query)
        cur.execute(query)
        print(cur.fetchall()[0][0])       

# %%
def main():
    '''Loads the staging, fact and dimension tables 
       by executing load_staging_tables and insert_tables functions'''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    printOutput(cur)
    conn.close()
    
main()


