import os
import argparse
import glob
from configparser import ConfigParser

# Based on the NSODP sensor string downloader (CMAR_MAIN)
# Use deployment info to generate the fixed values (https://data.novascotia.ca/Fishing-and-Aquaculture/Nova-Scotia-Current-and-Wave-Data-Deployment-Infor/uban-q9i2/about_data)

from data_tools import CMAR_fetch_data, CSV_split_by_station, CMAR_data_converter, CMAR_fetch_metadata, CMAR_generate_xml

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

def main(output_directory, config_file, skip_fetch_data=False, skip_split_stations=False, skip_merge_data=False, skip_create_metadata=False, skip_create_datasetxml=False):
    # example list
    # deployment_id_list = uban-q9i2
    # dataset_id_list = ['hrs3-qwwh', '7iig-p4yk', 'd5r3-x2vi', '9gpk-gb2v', '3szy-egwz', 'mzsa-kbqq', 'sf72-xdbr', 'gw4c-5af6', 'x5q4-n7kq', 'gi34-iea2', '9339-jb3s']
    
    # To update as datasets are updated to new structure
    
    config = ConfigParser()
    config.read(config_file)

    deployment_id = '
    print("Fetching data for dataset: %s" % deployment_id)
    raw_output_filename = setup_raw_output_filename(
        output_directory,
        deployment_id
    )
    deployment_data = CMAR_fetch_data.main(deployment_id, raw_output_filename, 'data_types_wave_deployment.yaml', 'deployment_id, deployment_date')
    print(deployment_data)
    # Store the information here, just in case
    # Join data with this list and the individual data points?

    dataset_id_list = ['hrs3-qwwh', '7iig-p4yk', 'd5r3-x2vi', '9gpk-gb2v', '3szy-egwz', 'mzsa-kbqq', 'sf72-xdbr', 'gw4c-5af6', 'x5q4-n7kq', 'gi34-iea2', '9339-jb3s', 'uban-q9i2']
    for dataset_id in dataset_id_list:
        # Create a new folder inside of output_directory for each dataset
        dataset_output_directory = os.path.join(output_directory, dataset_id)

        print("Fetching data for dataset: %s" % dataset_id)
        raw_output_filename = setup_raw_output_filename(
            dataset_output_directory,
            dataset_id
        )
        
        if not(skip_fetch_data):
            CMAR_fetch_data.main(dataset_id, raw_output_filename, 'data_types_wave.yaml', 'deployment_id, timestamp_utc')

        if not os.path.exists(raw_output_filename):
            raise Exception("DatasetID: %s returned no dataset. Make sure the datasetID exists in data.novascotia.ca" % dataset_id)

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
            if not(skip_split_stations):
                for input_filename in glob.glob(os.path.join(station_output_directory, "*.csv")):
                    CMAR_data_converter.main(input_filename, merged_output_directory)
            else:
                CMAR_data_converter.main(raw_output_filename, merged_output_directory)

        if not(skip_create_metadata):
            for data_file in glob.glob(os.path.join(merged_output_directory, "*.csv")):
                print("fetching metadata")
                CMAR_fetch_metadata.main(dataset_id, data_file, merged_output_directory)

        if not(skip_create_datasetxml):
            for data_file in glob.glob(os.path.join(merged_output_directory, "*.csv")):
                CMAR_generate_xml.main(dataset_id, data_file, merged_output_directory)




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
    parser.add_argument("--skip_create_datasetxml",
                    help="Skip the creation of dataset.xml", action="store_true")                

    args = parser.parse_args()
    output_directory = args.output_directory
    main(
        output_directory,
        args.skip_fetch_data,
        args.skip_split_stations,
        args.skip_merge_data,
        args.skip_create_metadata,
        args.skip_create_datasetxml
    )