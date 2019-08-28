import psycopg2
import boto3
from sql_queries import create_table_queries, drop_table_queries
from utils import open_tcp_connection, build_connection_str


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    connection_str = build_connection_str()
    print(connection_str)
    conn = psycopg2.connect(connection_str)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()