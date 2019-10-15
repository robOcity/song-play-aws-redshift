"""
Connects to AWS Redshift and drops and re-creates all tables.
"""

from sql_queries import create_table_queries, drop_table_queries
from utils import connect


def drop_tables(cur, conn):
    """Drops all tables from the database.

    Arguments:
    cur  -- the database cursor object for accessing data
    conn -- the connection to the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print(f"\nRunning: {query}")


def create_tables(cur, conn):
    """Creates both staging and star-schema tables.

    Arguments:
    cur  -- the database cursor object for accessing data
    conn -- the connection to the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print(f"\nRunning: {query}")


def main():
    """Drops and re-creates all tables."""
    cur, conn = connect()
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
