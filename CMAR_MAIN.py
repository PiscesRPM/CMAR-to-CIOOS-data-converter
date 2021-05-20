import os
import pandas as pd
from sodapy import Socrata
import sys
import argparse
from pathlib import Path
import argparse

from data_tools import CMAR_fetch_data, CSV_split_by_station, CMAR_data_converter, CMAR_fetch_metadata

def setup_raw_output_filename(dataset_output_directory, dataset_id):
    """Builds a pathname for a raw output file, creates directories as needed

    Args:
        dataset_output_directory (String): The output directory for the overall dataset
        dataset_id (String): The dataset ID for the current dataset

    Returns:
        String: The filename including path for the raw output file
    """    
    # Create the dataset_output_directory if it doesn't exist
    if not(os.path.exists(dataset_output_directory)):
        os.mkdir(dataset_output_directory)
    
    # Create an output filename for the raw data, including the path to the directory
    raw_output_filename = os.path.join(
        dataset_output_directory,
        '%s_raw.csv' % dataset_id
    )
    return raw_output_filename

def main(output_directory):
    # example list
    dataset_id_list = ['eb3n-uxcb','x9dy-aai9']
    for dataset_id in dataset_id_list:
        # Create a new folder inside of output_directory for each dataset
        dataset_output_directory = os.path.join(output_directory, dataset_id)

        print("Fetching data for dataset: %s" % dataset_id)
        raw_output_filename = setup_raw_output_filename(
            dataset_output_directory,
            dataset_id
        )
        CMAR_fetch_data.main(dataset_id, raw_output_filename)
        # raw_file_name = 'data_tools\%s\%s' % (set_id,set_id + '_raw.csv')
        # CSV_split_by_station.main(raw_file_name,set_id)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("output_directory", type=str,
                    help="Output directory for data and metadata")
    args = parser.parse_args()
    output_directory = args.output_directory
    main(output_directory)