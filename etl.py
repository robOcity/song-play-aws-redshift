import configparser
import psycopg2
import sys
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn, config):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read("dwh.cfg")
    fmt_config_str = "host={} dbname={} user={} password={} port={}".format(
        *config["CLUSTER"].values()
    )
    print(f"fmt_config_str={fmt_config_str}")
    conn = psycopg2.connect(fmt_config_str)
    cur = conn.cursor()

    load_staging_tables(cur, conn, config)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
