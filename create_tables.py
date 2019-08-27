import configparser
import psycopg2
import boto3
from sql_queries import create_table_queries, drop_table_queries

def open_tcp_connection():
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(type(defaultSg))
        
        defaultSg.authorize_ingress(
            GroupName='redshift_security_group', 
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
       
    except Exception as e:
        print(e)
        raise e

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