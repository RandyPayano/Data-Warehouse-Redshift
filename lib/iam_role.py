import boto3
import configparser
import json
from lib.config_update import config_update


def create_iam_role(cfg_file_path, iam):
    """Creates AWS iam role
    Args:
      config (string): Path to config file
      iam (Iam cliet): AWS iam client 
    Returns:
      dictionary: IAM Role Information
    """
    print("-" * 15, "Creating Role")
    print("")
    # Get config file from path
    cfg_file = configparser.ConfigParser()
    cfg_file.read_file(open(cfg_file_path))
    RoleName = cfg_file.get('DWH', 'DWH_IAM_ROLE_NAME')
    Policy_Arn = cfg_file.get('AWS','PolicyArn')

    # Checking role does not already exists
    try:
        response = iam.get_role(RoleName=RoleName)
        print('iam Role already exists: ' + response['Role']['Arn'])
        return response
    except:
        response = None

    # Creating role
    if response is None:
        try:
            dwhRole = iam.create_role(
            RoleName = RoleName,
            Description = 'Allows Redshift to call AWS services on your behalf',
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
               'Version': '2012-10-17'})
            )

            # Attaching Policy to Role (defined in configuration file)
            print(f"Attaching Policy: {Policy_Arn}")
            iam.attach_role_policy(
                RoleName = RoleName,
                PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
            )
            
            # Store Role Arn
            RoleArn = iam.get_role(RoleName=RoleName)['Role']['Arn']
            # Save value into config
            config_update('func.cfg', 'AWS', 'RoleArn', RoleArn)
            print(f"Created: IAM Role: {RoleName}, Policy attached: {Policy_Arn}")
            print("")
            return dwhRole

        except Exception as e:
          print(f"Error: {e}")


def delete_iam_role(cfg_file_path, iam):
    """Deletes AWS role and detach policy
    Args:
      config_path (string): Path to config file
      iam (Iam cliet object): AWS iam client 
    Returns:
      string: Deletion confirmation or Error
    """
    print("-" * 15, "Deleting Role")

    # Get config file from path
    
    cfg_file = configparser.ConfigParser()
    cfg_file.read_file(open(cfg_file_path))
    RoleName = cfg_file.get('DWH', 'DWH_IAM_ROLE_NAME')
    Policy_Arn = cfg_file.get('AWS','PolicyArn')

    # Detach role policy & DELETE role
    try:
        response = iam.detach_role_policy(RoleName=RoleName, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"), iam.delete_role(RoleName=RoleName)
        
        config_update('func.cfg', 'AWS', "RoleArn", "<>")
        print(f"Policy detached successfully: {Policy_Arn}")
        print(f"Role deleted successfully: {RoleName}")
        print("")
        return response
    except Exception as e:
        print(f"Error: {e}")

    


    return None
  
  