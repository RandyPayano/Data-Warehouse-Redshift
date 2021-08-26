import configparser
from botocore.exceptions import ClientError

def config_update(cfg_file_path, section, key, new_value):
    """Updates configuration file
    Args:
        cfg_file (configparser object): Configuration file to update
        section (string): Section where the Key resides
        key (string): Key to be updated in configuration file
        value (string): New value to be assigned to the key
    """
    try:
        # Read cfg_file
        config = configparser.ConfigParser()
        config.read(cfg_file_path)

       # Select section and write to file
        config.set(section, key, new_value)

        with open(cfg_file_path, 'w') as f:
            config.write(f)
        print(f"Configuration file updated Key: {key} | Value: {new_value}")
    except ClientError as e:
        print(e)


