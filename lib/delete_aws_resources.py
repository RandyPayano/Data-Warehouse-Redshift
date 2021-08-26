  
import boto3
import configparser
from lib.iam_role import delete_iam_role
from lib.redshift_cluster import delete_redshift_cluster, wait_for_cluster_deletion
from lib.vpc_security_group import delete_security_group

## Reading Configuration Params
config_path = 'func.cfg'
config = configparser.ConfigParser()
config.read_file(open(config_path))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

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

ec2 = boto3.client("ec2",
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )


def delete_aws_resources():
    """Deletes all AWS Resources
    """
    print("-" * 15, "Deleting AWS resources")
    delete_redshift_cluster(config_path,redshift_client)
    wait_for_cluster_deletion(config_path, redshift_client)
    delete_iam_role(config_path, iam_client)
    delete_security_group(config_path, ec2)
    print("-" * 15, "All AWS resources have been deleted")
    print("")
if __name__ == "__main__":
    delete_aws_resources()
