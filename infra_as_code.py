"""
infra_as_code.py

This script aims to create AWS Redshift Cluster and AWS IAM role(s) in the form of Infrastructure as Code

Author: Jonathan Cen
"""

import boto3
import configparser
import json
import pandas as pd

config = configparser.ConfigParser()
config.read_file(open('dwh-jc.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")  # multi-node
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")  # 4
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")  # dc2.large
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")  # dwhCluster
DWH_DB = config.get("DWH", "DWH_DB")  # dwh
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")  # dwhuser
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")  # Passw0rd
DWH_PORT = config.get("DWH", "DWH_PORT")  # 5439
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")  # dwhStudentRole

# connect with clients
iam = boto3.client('iam', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
redshift = boto3.client('redshift', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
ec2 = boto3.resource('ec2', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
def create_IAM_role():
    try:
        print('1.1 Creating a new IAM Role')
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allow Redshift cluster to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    'Statement': [
                        {
                            'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {'Service': 'redshift.amazonaws.com'}
                        }
                    ],
                    'Version': '2012-10-17'
                }
            )
        )
    except Exception as e:
        print(e)


def attach_policy():
    try:
        print('1.2 Attaching Policy')
        iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )["ResponseMetadata"]["HTTPStatusCode"]
    except Exception as e:
        print(e)


def get_role_arn():
    try:
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    except Exception as e:
        print(e)
    return roleArn


def create_redshift_cluster(roleArn):
    try:
        response = redshift.create_cluster(
            # TODO: add parameters for hardware
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            # TODO: add parameters for identifiers & credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # TODO: add parameter for role (to allow s3 access)
            IamRoles=[roleArn]
        )
    except Exception as e:
        print(e)

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def describe_redshift_cluster():
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    prettyRedshiftProps(myClusterProps)
    return myClusterProps


def open_tcp(myClusterProps):
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)


def delete_redshift_cluster():
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)


def delete_role():
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)


if __name__ == "__main__":
    # create_IAM_role()   # this only needs to be run once
    # attach_policy()     # this only needs to be run once
    roleArn = get_role_arn()
    create_redshift_cluster(roleArn)  # run this to create a redshift cluster.
    myClusterProps = describe_redshift_cluster()
    print(prettyRedshiftProps(myClusterProps))

    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    # open_tcp(myClusterProps)
    delete_redshift_cluster()   # run this to delete the redshift cluster.



