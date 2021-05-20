import os
import pandas as pd
from sodapy import Socrata
import sys
import argparse
from pathlib import Path
import argparse
import glob

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

def main(output_directory, skip_fetch_data=False, skip_split_stations=False, skip_merge_data=False, skip_create_metadata=False):
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

        if not(skip_fetch_data):
            CMAR_fetch_data.main(dataset_id, raw_output_filename)

        station_output_directory = os.path.join(dataset_output_directory, "data_by_station")
        if not(os.path.exists(station_output_directory)):
            os.mkdir(station_output_directory)

        if not(skip_split_stations):
            CSV_split_by_station.main(raw_output_filename, station_output_directory)

        merged_output_directory = os.path.join(dataset_output_directory, "final_output")
        if not(os.path.exists(merged_output_directory)):
            os.mkdir(merged_output_directory)

        # Iterate through every CSV file in station_output_directory and merge
        if not(skip_merge_data):
            for input_filename in glob.glob(os.path.join(station_output_directory, "*.csv")):
                CMAR_data_converter.main(input_filename, merged_output_directory)

        if not(skip_create_metadata):
            for data_file in glob.glob(os.path.join(merged_output_directory, "*.csv")):
                CMAR_fetch_metadata.main(dataset_id, data_file, merged_output_directory)






if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("output_directory", type=str,
                    help="Output directory for data and metadata")

    parser.add_argument("--skip_fetch_data",
                    help="Skip fetching new data", action="store_true")
    parser.add_argument("--skip_split_stations",
                    help="Skip splitting data by station", action="store_true")
    parser.add_argument("--skip_merge_data",
                    help="Skip merging data to make files CIOOS compliant", action="store_true")
    parser.add_argument("--skip_create_metadata",
                    help="Skip the creation of metadata", action="store_true")

    args = parser.parse_args()
    output_directory = args.output_directory
    main(
        output_directory,
        args.skip_fetch_data,
        args.skip_split_stations,
        args.skip_merge_data,
        args.skip_create_metadata
    )