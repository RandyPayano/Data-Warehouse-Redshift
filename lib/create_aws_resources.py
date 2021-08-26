import boto3
import configparser
from lib.redshift_cluster import create_redshift_cluster, wait_for_cluster_creation
from lib.iam_role import create_iam_role
from lib.vpc_security_group import create_security_group

## Reading Configuration Params
config_path = 'func.cfg'
config = configparser.ConfigParser()
config.read_file(open(config_path))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

Group_Name                = config.get('SECURITY', 'sgroup_name')
## Creating Redshift, S3 and IAM, EC2 clients

iam_client = boto3.client("iam",
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )

redshift_client = boto3.client("redshift",
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )

s3 = boto3.resource("s3",
                    region_name="us-west-2",
                    aws_access_key_id=config.get('AWS','KEY'),
                    aws_secret_access_key=config.get('AWS','SECRET')
                    )

ec2_client = boto3.client("ec2",
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )


def create_aws_resources():
    """Creates all aws resources needed for ETL process."
    """
    print("-" * 15, "Creating AWS Resources")
    create_iam_role(config_path, iam_client)
    create_redshift_cluster(config_path, redshift_client)
    wait_for_cluster_creation(config_path, redshift_client)
    create_security_group(config_path, ec2_client, redshift_client)
    print("AWS Resources created successfully")
                     
if __name__ == "__main__":
    create_aws_resources()
