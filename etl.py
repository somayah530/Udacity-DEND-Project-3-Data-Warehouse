import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """
    This function load JSON data from S3 bucket to the staging table in Redshift
    ------
    cur : cursor variable of the database (connection object on redshift) 
    conn: connection object on the redshift
    ------
    Output:
    
    song_data in staging_songs table.
    log_data in staging_events table.
   
    """
    
    for query in copy_table_queries:
        print('Loading data by: '+query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    """
    This function insert data from staging tables to the dimension and fact tables.
    -----
    cur : cursor variable of the database (connection object on redshift) 
    conn: connection object on the redshift (host, database name, user, password, port) to connect the database.
    ----
    Output:
    
      Data inserted from staging tables to dimension tables.
      Data inserted from staging tables to fact table.
    """
    
    for query in insert_table_queries:
        print('Transform data by: '+query)
        cur.execute(query)
        conn.commit()


def main():
    
    
    """
    This function connect to database and call (load_staging_tables and insert_tables)
    in the end it closes the database connection

    """
        
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to redshift')
    cur = conn.cursor()
    
    print('Loading staging tables')
    #load_staging_tables(cur, conn)
    
    print('Transform from staging')
    insert_tables(cur, conn)

    conn.close()
    print('ETL Ended')


if __name__ == "__main__":
    main()