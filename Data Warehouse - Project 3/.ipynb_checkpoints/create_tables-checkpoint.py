import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Method to drop all the tables in Redshift database for the given cursor and connection
        
    :param cursor cur: The cursor
    :param connection conn: The connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ 
    Method to create all the tables in Redshift database for the given cursor and connection
        
    :param cursor cur: The cursor
    :param connection conn: The connection
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ 
    Main method which will connect to redshift, drop the tables and create all of them
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print(*config['CLUSTER'].values())

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()