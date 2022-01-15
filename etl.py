"""
etl.py

Student: Jonathan Cen
Description: This script is responsible for loading data from S3 into staging tables on Redshift and then process that data into the analytic tables on Redshift
"""

import psycopg2
import configparser
import time
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    This function loads song data and log data (both in json format) from S3 onto their corresponding staging tables (staging_songs and staging_events) on the sparkify data warehouse.
    :param cur: this is a connection cursor to the sparkify data warehouse
    :param conn: this the connection to the the sparkify data warehouse
    :returns: None, this function does not return any output.
    """
    for query in copy_table_queries:
        print("Loading data into staging table '{}'...".format(query.split()[1]))
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function executes queries that transform data in the staging_songs table and the staging_events table, and then insert the data into the 5 dimensional table
    :param cur: this is a connection cursor to the sparkify data warehouse
    :param conn: this the connection to the the sparkify data warehouse
    :returns: None, this function does not return any output.
    """
    for query in insert_table_queries:
        print("Inserting data into table '{}'...".format(query.split()[2]))
        cur.execute(query)
        conn.commit()


def main():
    """
    This function controls the main logic of the etl process. It starts by extracting the required connection parameter from the config file, dwh.cfg and establish a connection with the sparkify data warehouse.
    It then executes the load_staging_tables to populate the staging tables, followed by executing the insert_tables() to insert data into the 5 dimensional tables by passing in the cursor and connection.
    This function eventually closes the connection when the ETL process is completed.
    :returns: None, this function does not return any output.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("Time used = {} seconds.".format(round(time.time() - start_time, 2)))