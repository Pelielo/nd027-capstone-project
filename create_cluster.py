import configparser
import json
import time

import boto3
import pandas as pd
import schedule


def create_client(service_name, region_name, key, secret):
    print(f"Creating {service_name} client")

    client = boto3.client(
        service_name=service_name,
        region_name=region_name,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )

    return client


def create_resource(service_name, region_name, key, secret):
    print(f"Creating {service_name} resource")

    client = boto3.resource(
        service_name=service_name,
        region_name=region_name,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )

    return client


def create_iam_role(iam_client, iam_role_name):
    # Create the role
    try:
        print("Creating a new IAM Role")
        iam_client.create_role(
            Path="/",
            RoleName=iam_role_name,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except Exception as e:
        print(e)

    print("Attaching Policy")

    iam_client.attach_role_policy(
        RoleName=iam_role_name,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )["ResponseMetadata"]["HTTPStatusCode"]

    print("Getting the IAM role ARN")
    roleArn = iam_client.get_role(RoleName=iam_role_name)["Role"]["Arn"]

    print(f"Role ARN: {roleArn}")

    return roleArn


def create_cluster(
    redshift_client,
    roleArn,
    cluster_type,
    node_type,
    num_nodes,
    db_name,
    cluster_identifier,
    db_user,
    db_password,
):
    try:
        print(
            f"""Creating Redshift cluster with following properties:
                     cluster type: {cluster_type}, node type: {node_type}, 
                     number of nodes: {num_nodes}, DB name: {db_name}, 
                     cluster identifier: {cluster_identifier}"""
        )

        redshift_client.create_cluster(
            # HW
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=int(num_nodes),
            # Identifiers & Credentials
            DBName=db_name,
            ClusterIdentifier=cluster_identifier,
            MasterUsername=db_user,
            MasterUserPassword=db_password,
            # Roles (for s3 access)
            IamRoles=[roleArn],
        )

        print("Cluster starting...")
    except Exception as e:
        print(e)


def prettyRedshiftProps(props):
    pd.set_option("display.max_colwidth", -1)
    keysToShow = [
        "ClusterIdentifier",
        "NodeType",
        "ClusterStatus",
        "MasterUsername",
        "DBName",
        "Endpoint",
        "NumberOfNodes",
        "VpcId",
    ]
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def check_cluster_properties(redshift_client, cluster_identifier):
    return redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)[
        "Clusters"
    ][0]


def authorize_cluster_access(ec2_resource, vpc_id, db_port):
    print(f"Authorizing inbound access to port {db_port}")
    try:
        vpc = ec2_resource.Vpc(id=vpc_id)
        defaultSg = list(vpc.security_groups.all())[0]

        print(f"Default Security Group: {defaultSg}")

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(db_port),
            ToPort=int(db_port),
        )
    except Exception as e:
        print(e)


def main():
    config = configparser.ConfigParser()
    config.read_file(open("settings.cfg"))

    KEY = config.get("AWS", "KEY")
    SECRET = config.get("AWS", "SECRET")

    DWH_CLUSTER_TYPE = config.get("CLUSTER", "CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("CLUSTER", "NUM_NODES")
    DWH_NODE_TYPE = config.get("CLUSTER", "NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER", "CLUSTER_IDENTIFIER")
    DWH_DB = config.get("CLUSTER", "DB_NAME")
    DWH_DB_USER = config.get("CLUSTER", "DB_USER")
    DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
    DWH_PORT = config.get("CLUSTER", "DB_PORT")

    DWH_IAM_ROLE_NAME = config.get("CLUSTER", "IAM_ROLE_NAME")

    redshift = create_client("redshift", "us-west-2", KEY, SECRET)
    iam = create_client("iam", "us-west-2", KEY, SECRET)
    ec2 = create_resource("ec2", "us-west-2", KEY, SECRET)

    roleArn = create_iam_role(iam, DWH_IAM_ROLE_NAME)

    create_cluster(
        redshift,
        roleArn,
        DWH_CLUSTER_TYPE,
        DWH_NODE_TYPE,
        DWH_NUM_NODES,
        DWH_DB,
        DWH_CLUSTER_IDENTIFIER,
        DWH_DB_USER,
        DWH_DB_PASSWORD,
    )

    def ensure_cluster_available():
        cluster_props = check_cluster_properties(redshift, DWH_CLUSTER_IDENTIFIER)

        # print(cluster_props)

        cluster_status = cluster_props["ClusterStatus"]

        if cluster_status == "available":
            print("Redshift cluster available!")
            return schedule.CancelJob

    schedule.every(30).seconds.do(ensure_cluster_available)

    while True:
        schedule.run_pending()
        if schedule.jobs == []:
            break
        time.sleep(5)

    cluster_props = check_cluster_properties(redshift, DWH_CLUSTER_IDENTIFIER)

    DWH_ENDPOINT = cluster_props["Endpoint"]["Address"]
    DWH_ROLE_ARN = cluster_props["IamRoles"][0]["IamRoleArn"]
    print(f"DWH_ENDPOINT: {DWH_ENDPOINT}")
    print(f"DWH_ROLE_ARN: {DWH_ROLE_ARN}")

    authorize_cluster_access(ec2, cluster_props["VpcId"], DWH_PORT)


if __name__ == "__main__":
    main()
