# %%
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# %%
def drop_tables(cur, conn):
    '''Drops all the staging, fact and dimension tables if they exist
       by executing queries in drop_table_queries array'''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''creates the staging, fact and dimesion tables 
       by executing queries in create_table_queries array'''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

# %%
def main():
  '''Drops if tables exist and creates the staging, fact and dimesion tables 
     by executing drop_tables and create_tables functions'''
  config = configparser.ConfigParser()
  config.read('dwh.cfg')

  conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
  cur = conn.cursor()
    
  drop_tables(cur, conn)
  create_tables(cur, conn)

  conn.close()


# if __name__ == "__main__":
main()


