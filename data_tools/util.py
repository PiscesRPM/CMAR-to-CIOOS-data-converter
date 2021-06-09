import os

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