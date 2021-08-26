from botocore.exceptions import ClientError
import pandas as pd 
from lib.config_update import config_update
import configparser

def create_security_group(cfg_file_path, ec2_client, redshift_client):

    # Read cfg_file
    config = configparser.ConfigParser()
    config.read(cfg_file_path)

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    Group_Name                = config.get('SECURITY', 'sgroup_name')

    def get_VpcId(props):
      keysToShow = ['VpcId']
      x = [(k, v) for k,v in props.items() if k in keysToShow]
      return pd.DataFrame(data=x, columns=["Key", "Value"])

    myClusterProps = redshift_client.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    

   
    try: 
        response = ec2_client.describe_security_groups(Filters= [{"Name": "group-name", "Values": [Group_Name]}])
        
    except ClientError as e:
        print(f"Error: {e}")
        
    if len(response['SecurityGroups']) > 0:
        print(f'Security Group already exists, Group Name: {Group_Name} Groupid: ' + response['SecurityGroups'][0]['GroupId'])
        return response['SecurityGroups'][0]['GroupId']
    
    else:
        response = None
        
    
    if response is None:
        # Assuming the security goroup doesn't exist, go ahead and create it
        print("-" * 15,"Creating Security Group")
        print("")
        try:
            ###### Make sure security group name is in config
            response = ec2_client.create_security_group(GroupName= Group_Name,
                                                 Description='Redshift security group',
                                                 VpcId=get_VpcId(myClusterProps)['Value'][0])
           
            security_group_id = response['GroupId']
            print(security_group_id, get_VpcId(myClusterProps)['Value'][0])

           
            ec2_client.authorize_security_group_ingress(
            GroupId=str(security_group_id),
            CidrIp='0.0.0.0/0',  
            IpProtocol='TCP',  
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
            config_update(cfg_file_path, 'SECURITY', 'sgroup_id', security_group_id)
            config_update(cfg_file_path, 'SECURITY', 'vpc_id', get_VpcId(myClusterProps)['Value'][0])
            print("Security Group created")
            print("")
            return security_group_id
        except Exception as e:
            print(e)            

            
            

def delete_security_group(cfg_file_path, ec2_client):
    """Deletes AWS VPC Security Group
    Args:
        config (ConfigParser object): Configuration File to define Resource configuration
    """
    # Read cfg_file
    config = configparser.ConfigParser()
    config.read(cfg_file_path)
    Group_Name = config.get('SECURITY', 'sgroup_name')

    # Delete security group
    print("-"*15, "Deleting security group")
    try:
        response = ec2_client.delete_security_group(GroupName=Group_Name)
        print(f'Security Group Deleted, Name: {Group_Name}')
        config_update('func.cfg', "SECURITY", "sgroup_name", "choose-sgroup")
        config_update('func.cfg', "SECURITY", "sgroup_id", "<LEAVE BLANK>")
        config_update('func.cfg', "SECURITY", "vpc_id", "<LEAVE BLANK>")
        print("")
    except ClientError as e:
        print(f"ERROR {e}")