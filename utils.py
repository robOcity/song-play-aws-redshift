"""Establish connections to AWS Redshift."""

import configparser
import psycopg2


def get_config():
    """Returns a ConfigParser object for access to configuration parameters."""
    return configparser.ConfigParser(interpolation=None)


def build_connection_str(cfg_file=".env/dwh.cfg"):
    """Returns the formatted PostgreSQL connection string.

    Keyword arguments:
    cfg_file -- configuration filename (default=.env/dwh.cfg)
    """

    config = get_config()
    config.read(cfg_file, encoding="utf-8")
    DB_USER = config.get("CLUSTER", "DB_USER")
    DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
    DB_ENDPOINT = config.get("CLUSTER", "DB_ENDPOINT")
    DB_PORT = config.get("CLUSTER", "DB_PORT")
    DB_NAME = config.get("CLUSTER", "DB_NAME")

    return "postgresql://{}:{}@{}:{}/{}".format(
        DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB_NAME
    )


def connect():
    """Connects to Redshift cluster using values from configuration file."""
    connection_str = build_connection_str()
    print(f"\nConnecting: {connection_str}")
    conn = psycopg2.connect(connection_str)
    cur = conn.cursor()
    return cur, conn
