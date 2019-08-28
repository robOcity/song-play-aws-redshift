import configparser

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

    return "postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB_NAME)
