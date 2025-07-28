import os
import yaml

def check_raw_file_extension(raw_filename):
    """Checks that the raw file is a CSV, throws an error if not

    Args:
        raw_filename (String): The path to a file

    Raises:
        Exception: Throws an exception if the file is the wrong type
    """    
    raw_file_extension = os.path.splitext(raw_filename)[1]
    if (raw_file_extension != ".csv"):
        raise Exception("File must have a csv extension. \nExample:python CSV_to_Unique_Stations.py -f data.csv")

def load_dtypes_file(config, config_section='main'):
    dtype_config = {}
    if 'dtype_config_file' in config[config_section]:
        dtype_file = config[config_section]['dtype_config_file']
        dtype_file_location = os.path.join(os.path.dirname(__file__), '..', dtype_file)
        if os.path.exists(dtype_file_location):
            with open(dtype_file_location) as f:
                dtype_config = yaml.load(f, Loader=yaml.FullLoader)
        else:
            print("No datatype config found")
    return dtype_config