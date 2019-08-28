import psycopg2
import sys
from sql_queries import copy_table_queries, insert_table_queries
from utils import open_tcp_connection, build_connection_str, get_config


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    connection_str = build_connection_str()
    print(f"fmt_config_str={connection_str}")
    conn = psycopg2.connect(connection_str)
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
