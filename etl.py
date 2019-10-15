"""Extracts, transforms and loads data from S3 to star-schema tables."""

from sql_queries import copy_table_queries, insert_table_queries
from utils import connect


def load_staging_tables(cur, conn):
    """Extracts data from JSON files and loads into staging tables.

    Arguments:
    cur  -- the database cursor object for accessing data
    conn -- the connection to the database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print(f"\nRunning: {query}")


def insert_tables(cur, conn):
    """Transforms and loads data from staging to star schema tables.
    
    
    Arguments:
    cur  -- the database cursor object for accessing data
    conn -- the connection to the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print(f"\nRunning: {query}")


def get_command():
    """Loads new data and updates existing (transform & load) data."""
    cmd = ""
    while cmd not in ["L", "I"]:
        cmd = input("\nL - load\nI - insert\nCommand: ")[0].upper()
    return cmd


def main():
    """Extracts or transforms and loads the data at the users request."""

    cmd = get_command()
    if cmd == "L":
        # load data
        print("\nLoading data (~20 minutes)")
        cur, conn = connect()
        load_staging_tables(cur, conn)
    elif cmd == "I":
        # insert data
        print("\nInserting data (~2 minutes)")
        cur, conn = connect()
        insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
