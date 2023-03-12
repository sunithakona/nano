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


    conn.close()


main()

# %%
import os 
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
KEY=config.get('AWS','key')
SECRET= config.get('AWS','secret')

DWH_DB= config.get("CLUSTER","DB_NAME")
DWH_DB_USER= config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD= config.get("CLUSTER","DB_PASSWORD")
DWH_PORT = config.get("CLUSTER","DB_PORT")
DWH_ENDPOINT = config.get("CLUSTER","HOST")

%load_ext sql
conn_string='postgresql://{}:{}@{}:{}/{}'.format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)
%sql $conn_string



# %%
%%sql
SELECT COUNT(*) FROM staging_events;

# %%
%%sql
select COUNT(*) from staging_songs;

# %%
%%sql
SELECT COUNT(*) FROM users;

# %%
%%sql
select COUNT(*) from songs;

# %%
%%sql
SELECT COUNT(*) FROM artists;

# %%
%%sql
select COUNT(*) from time;

# %%
%%sql
select COUNT(*) from songplay;


