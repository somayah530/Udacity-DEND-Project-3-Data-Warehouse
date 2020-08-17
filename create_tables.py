import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
    """
        Executes all the drop table queries (drop any existing tables from sparkifydb).
        --------
        cur : cursor variable of the database (connection object)
        conn: connection variable of the database (cursor object)
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    
    """
        Executes all the create table queries (Create table in the Redshift cluster).
        --------
        cur : cursor variable of the database (connection object)
        conn: connection variable of the database (cursor object)
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """
        This function combine all the important command related to connects database using credentials in the config file 
        Connect to AWS Redshift, create new database (sparkifydb),drop any existing tables, create new tables. Close database connection.
       
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()