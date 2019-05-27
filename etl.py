import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Moves the data from Amazon S3 to the staging tables.
    The commands for this process are defined in copy_table_queries.
    :param cur:
        The cursor to execute statements
    :param conn:
        The connection to the database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Completes the ETL process by moving the data from the staging tables to the final tables.
    The commands for this process are defined in insert_table_queries
    :param cur:
        The cursor to execute statements
    :param conn:
        The connection to the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to Redshift. Copies the data from Amazon S3 to Redshift.
    Then completes the ETL process by moving the data from the staging tables to the final tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()