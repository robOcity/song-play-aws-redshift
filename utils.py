import configparser
import psycopg2


def get_config():
    return configparser.ConfigParser(interpolation=None)


def build_connection_str(cfg_file="dwh.cfg"):
    config = get_config()
    config.read(cfg_file, encoding='utf-8')
    DB_USER = config.get('CLUSTER', 'DB_USER')
    DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
    DB_ENDPOINT = config.get('CLUSTER', 'DB_ENDPOINT')
    DB_PORT = config.get('CLUSTER', 'DB_PORT')
    DB_NAME = config.get('CLUSTER', 'DB_NAME')

    return "postgresql://{}:{}@{}:{}/{}".format(
        DB_USER,
        DB_PASSWORD,
        DB_ENDPOINT,
        DB_PORT,
        DB_NAME)


def connect():
    connection_str = build_connection_str()
    print(f'\nConnecting: {connection_str}')
    conn = psycopg2.connect(connection_str)
    cur = conn.cursor()
    return cur, conn
