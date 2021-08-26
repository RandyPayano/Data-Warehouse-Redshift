import configparser
import time
from lib.config_update import config_update
def create_redshift_cluster(cfg_file_path, redshift_client):
    """Creates AWS redshift cluster
    Args:
        cfg_file_path (string): Path to configuration file
        redshift_client ([type]): [description]
    Returns:
        [type]: [description]
    """
    
    config = configparser.ConfigParser()
    config.read_file(open(cfg_file_path))
    roleArn                = config.get('AWS','rolearn')
    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DHW_ROLEARN =   config.get("DWH","RoleArn")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")

    print("-"*15, "Creating Cluster")
    # Check if cluster already exists
    try:
        response = redshift_client.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
        print('Cluster already exists: ' + response['Clusters'][0]['ClusterIdentifier'])
        return None
    except:
        response = None

    # If not create Cluster
    if response is None:
        try:
            response = redshift_client.create_cluster(        
                # hardware
                ClusterType=DWH_CLUSTER_TYPE,
                NodeType=DWH_NODE_TYPE,
                NumberOfNodes=int(DWH_NUM_NODES),   
                
                # identifiers & credentials
                    DBName=DWH_DB,
                    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                    MasterUsername=DWH_DB_USER,
                    MasterUserPassword=DWH_DB_PASSWORD,
                
                # parameter for role (to allow s3 access)
                IamRoles=[DHW_ROLEARN]
            
            )
 
            print("Redshift Cluster created")
            return response['Cluster']

        except Exception as e:
            print(f"Error {e}")
            return None

def wait_for_cluster_creation(cfg_file_path, redshift_client):
    """Verifies status of AWS redshift cluster and assures is available before proceeding
    Args:
      redshift_client (Client): Redshift Client
      cluster_id (string): AWS Redshift Cluster Name
    Returns:
      str: AWS Redshift Cluster Information
    """
    config = configparser.ConfigParser()
    config.read_file(open(cfg_file_path))

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

    try:
        
        while True:
          print("-"*15, "Checking Cluster Status")
          
          response = redshift_client.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
          cluster_info = response['Clusters'][0]
          cluster_status = cluster_info['ClusterStatus']
          print(f"Cluster status: {cluster_status}")
         
          
          if cluster_status  == 'available':
              print("")
              host_ = redshift_client.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['Endpoint']['Address']
              config_update(cfg_file_path, 'CLUSTER', 'HOST', host_)
              print("")
              break
          time.sleep(90)
        return None
    except:
        print("Cluster: {} not found.".format(DWH_CLUSTER_IDENTIFIER))
        response = None


    
        
    return None 

########## change
def delete_redshift_cluster(cfg_file_path, redshift_client):
    """Deletes AWS Redshift Cluster
    Args:
        config (ConfigParser object): Configuration File to define Resource configuration
    Returns:
        dictionary: AWS Redshift Information
    """
    config = configparser.ConfigParser()
    config.read_file(open(cfg_file_path))
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")


    print("-" * 15, "Deleting AWS Redshift Cluster")
    try:
        response = redshift_client.delete_cluster(
            ClusterIdentifier=config.get('DWH', 'dwh_cluster_identifier'),
            SkipFinalClusterSnapshot=True
        )
        print("")
    except:
        print(f"Redshift Cluster {DWH_CLUSTER_IDENTIFIER} does not exist!")
        print("")
        return None
    else:
        return response['Cluster']



def wait_for_cluster_deletion(cfg_file_path, redshift_client):
    """Verifies if AWS Redshift Cluster was deleted
    Args:
        cluster_id (dictionary): AWS Redshift Cluster Information
    """
    
    config = configparser.ConfigParser()
    config.read_file(open(cfg_file_path))
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

    print("-" * 15, "Waiting for Cluster deletion...")
    while True:
        try:
            redshift_client.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
        except:
            print("Cluster does not exist!")
            break
        else:
            print("")
            time.sleep(60)