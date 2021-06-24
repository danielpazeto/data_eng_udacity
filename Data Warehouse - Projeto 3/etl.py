import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ 
    Method to run the COPY command query and load the data from files to staging tables
        
    :param cursor cur: The cursor
    :param connection conn: The connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ 
    Method to run the inser queries to populate the tables
        
    :param cursor cur: The cursor
    :param connection conn: The connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ 
    Main method which will do the ETL process, it will connect to redshift, load the staging tables and insert in the rest of the tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Loading staging tables')
    load_staging_tables(cur, conn)
    
    print('Making the fact and dimensional tables')
    insert_tables(cur, conn)
    
    print('All tables were created')

    conn.close()


if __name__ == "__main__":
    main()