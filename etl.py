from sql_queries import copy_table_queries, insert_table_queries
from utils import connect


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'\nRunning: {query}')


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'\nRunning: {query}')


def get_command():
    cmd = ''
    while cmd not in [ 'L', 'I']:
        cmd = input('\nL - load\nI - insert\nCommand: ')[0].upper()
    return cmd


def main():

    cmd = get_command()
    if cmd == 'L':
        # load data
        print('\nLoading data (~20 minutes)')
        cur, conn = connect()
        load_staging_tables(cur, conn)
    elif cmd == 'I':
        # insert data 
        print('\nInserting data (~2 minutes)')
        cur, conn = connect()
        insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
