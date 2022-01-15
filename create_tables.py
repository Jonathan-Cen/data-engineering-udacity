"""
create_tables.py
Student: Jonathan Cen

Description: this script is responsible for creating fact and dimension tables for the star schema in Redshift.

** Be sure to switch to the us-west-2 region to view the cluster.
"""

import configparser
import psycopg2
import time
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function drops all tables (2 staging tables, 1 fact table, and 4 dimensional tables) if they exist.
    :param cur: this is a connection cursor to the sparkify data warehouse
    :param conn: this the connection to the the sparkify data warehouse
    :returns: None, this function does not return any output.
    """
    for query in drop_table_queries:
        print("Dropping Table '{}'...".format(query.split()[4][:-1]))
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function creates all tables (2 staging tables, 1 fact table, and 4 dimensional tables) if they not exist.
    :param cur: this is a connection cursor to the sparkify data warehouse
    :param conn: this the connection to the the sparkify data warehouse
    :returns: None, this function does not return any output.
    """
    for query in create_table_queries:
        print("Creating Table '{}'...".format(query.split()[5]))
        cur.execute(query)
        conn.commit()


def main():
    """
    This function controls the main logic of the create table and drop table process. It starts by extracting the required connection parameter from the config file, dwh.cfg and establish a connection with the sparkify data warehouse.
    It then executes the drop_tables() function to drop any tables if they exist, followed by executing the create_tables() function to insert data into the 1 fact table and 4 dimensional tables by passing in the cursor and connection.
    This function eventually closes the connection when the ETL process is completed.
    :returns: None, this function does not return any output.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("Time used = {} seconds.".format(round(time.time() - start_time, 2)))