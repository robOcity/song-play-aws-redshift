from sql_queries import create_table_queries, drop_table_queries
from utils import connect


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'\nRunning: {query}')


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'\nRunning: {query}')


def main():
    cur, conn = connect()
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
