import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


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

    connect_str = "host={} dbname={} user={} password={} port={}".format(*config["CLUSTER"].values())
    print(connect_str)
    conn = psycopg2.connect(connect_str)

    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()