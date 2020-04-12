import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError
import time


def create_client(service_name, region_name, key, secret):
    print(f"Creating {service_name} client")    

    client = boto3.client(service_name=service_name,
                            region_name=region_name,
                            aws_access_key_id=key,
                            aws_secret_access_key=secret
                            )

    return client


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def check_cluster_properties(redshift_client, cluster_identifier):
    props = redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
    return prettyRedshiftProps(props)


def kill_cluster(redshift_client, cluster_identifier, skip_snapshot):
    print("Deleting Redshift cluster in 10 seconds")
    #### CAREFUL!!
    time.sleep(10)
    redshift_client.delete_cluster(ClusterIdentifier=cluster_identifier, 
                                    SkipFinalClusterSnapshot=skip_snapshot)
    #### CAREFUL!!
    print("Cluster is being deleted...")


def delete_role(iam_client, role_name):
    print("Detaching role policy and deleting role")
    #### CAREFUL!!
    iam_client.detach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam_client.delete_role(RoleName=role_name)
    #### CAREFUL!!


def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","CLUSTER_IDENTIFIER")

    DWH_IAM_ROLE_NAME      = config.get("CLUSTER", "IAM_ROLE_NAME")
    

    redshift  = create_client('redshift', 'us-west-2', KEY, SECRET)
    iam  = create_client('iam', 'us-west-2', KEY, SECRET)

    kill_cluster(redshift, DWH_CLUSTER_IDENTIFIER, skip_snapshot=True)

    delete_role(iam, DWH_IAM_ROLE_NAME)

if __name__ == "__main__":
    main()