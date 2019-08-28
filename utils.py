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
