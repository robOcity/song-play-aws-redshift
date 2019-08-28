import configparser
import psycopg2
import boto3
from sql_queries import create_table_queries, drop_table_queries
from utils import open_tcp_connection


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():

    config = configparser.ConfigParser(interpolation=None)
    config.read("dwh.cfg", encoding='utf-8')
    DB_USER = config.get('CLUSTER', 'DB_USER')
    DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
    DB_ENDPOINT = config.get('CLUSTER', 'DB_ENDPOINT')
    DB_PORT = config.get('CLUSTER', 'DB_PORT')
    DB_NAME = config.get('CLUSTER', 'DB_NAME')

    connect_str="postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB_NAME)
    print(connect_str)
    conn = psycopg2.connect(connect_str)

    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()