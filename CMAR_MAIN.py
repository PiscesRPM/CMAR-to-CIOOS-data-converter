import os
import argparse
import glob
import yaml
import pandas as pd
from configparser import ConfigParser

from data_tools import CMAR_fetch_data, CSV_split_by_station, CMAR_data_converter, CMAR_fetch_metadata, CMAR_generate_xml, CMAR_join_datasets

def get_matching_files(directory, pattern):
    files = []

    for dir,_,_ in os.walk(directory):
        files.extend(glob.glob(os.path.join(dir,pattern))) 
    return files

def setup_raw_output_filename(dataset_output_directory, dataset_id):
    """Builds a pathname for a raw output file, creates directories as needed

    Args:
        dataset_output_directory (String): The output directory for the overall dataset
        dataset_id (String): The dataset ID for the current dataset

    Returns:
        String: The filename including path for the raw output file
    """    
    # Create an output filename for the raw data, including the path to the directory
    raw_output_filename = os.path.join(
        dataset_output_directory,
        '%s_raw.csv' % dataset_id
    )
    return raw_output_filename

def main(output_raw_directory, config_file, skip_fetch_data=False, skip_split_stations=False, skip_merge_data=False, skip_create_metadata=False, skip_create_datasetxml=False):
    # example list
    #dataset_id_list = ['eb3n-uxcb','x9dy-aai9','adpu-nyt8','eda5-aubu','knwz-4bap','9qw2-yb2f','v6sa-tiit','a9za-3t63','mq2k-54s4', 'wpsu-7fer']
    
    # To update as datasets are updated to new structure

    # Large datasets that may require multiple runs
    #'eb3n-uxcb', 'eda5-aubu','mq2k-54s4',

    configParser = ConfigParser()
    configParser.read(config_file)

    config = {}
    # Trying to remember the purpose of this
    for section in configParser.sections():
        config[section] = {}
        for option in configParser.options(section):
            config[section][option] = configParser.get(section, option)

    reference_dataset = None


    # Check if there is a reference dataset (ex. deployment information)
    if 'reference' in config:
        print('grabbing reference dataset')
        reference_dataset_id = config['reference']['reference_dataset_id']
        reference_dtype_file = config['reference']['dtype_config_file']
        reference_dataset_dir = os.path.join(output_directory, 'deployment_reference')
        reference_dataset_file = os.path.join(reference_dataset_dir, reference_dataset_id) + "_raw.csv"
        if not(skip_fetch_data):
            CMAR_fetch_data.main(reference_dataset_id, reference_dataset_dir, config, 'reference')
        reference_dataset = pd.read_csv(reference_dataset_file, dtype={config['metadata']['col_instrument_id']: 'category'})

    dataset_id_list = config['main']['dataset_id_list'].splitlines()
    for dataset_id in dataset_id_list:
        # Create a new folder inside of output_directory for each dataset
        dataset_directory = os.path.join(output_raw_directory, dataset_id)
        dataset_output_directory = os.path.join(dataset_directory, 'raw_data')

            # Create the dataset_output_directory if it doesn't exist
        if not(os.path.exists(dataset_output_directory)):
            os.makedirs(dataset_output_directory, exist_ok=True)

        print("Fetching data for dataset: %s" % dataset_id)
        raw_output_filename = setup_raw_output_filename(
            dataset_output_directory,
            dataset_id
        )
        
        if not(skip_fetch_data):
            CMAR_fetch_data.main(dataset_id, dataset_output_directory, config)

        process_dataset_files = get_matching_files(dataset_output_directory, "*_raw.csv")
        
        if len(process_dataset_files) < 1:
            raise Exception("DatasetID: %s returned no dataset. Make sure the datasetID exists in data.novascotia.ca" % dataset_id)

        recent_data_step = "_raw"
        recent_data_dir = "/raw_data/"

        # If there's a reference dataset, join with the downloaded dataset
        merged_output_directory = os.path.join(dataset_output_directory, "final_output")
        output_filename = raw_output_filename
        if reference_dataset is not None:
            if not reference_dataset.empty:
                recent_data_step = "_joined"
                recent_data_dir = "/joined_data/"
                reference_dataset_columns = config['reference']['dataset_columns'].splitlines()
                dataset= pd.read_csv(raw_output_filename)
                joined_df = CMAR_join_datasets.main(dataset, reference_dataset, config['reference']['join_key'], reference_dataset_columns)
                joined_output_filename = os.path.join(dataset_directory, "joined_data", dataset_id) + "_joined.csv"
                os.makedirs(os.path.dirname(joined_output_filename), exist_ok=True)
                joined_df.to_csv(joined_output_filename, index=False)
                process_dataset_files = get_matching_files(dataset_directory, "*_joined.csv")
                

        
        final_output_directory = dataset_directory.replace("/raw/", "/erddap/")
        os.makedirs(final_output_directory, exist_ok=True)

        # Use recent data step / dir to work with the most processed version of the dataset
        for process_file in process_dataset_files:
            converted_file = process_file.replace(recent_data_step, "_final").replace(recent_data_dir, "/final_output/").replace("/data/raw/", "/data/erddap/")
            os.makedirs(os.path.dirname(converted_file), exist_ok=True)
            CMAR_data_converter.main(process_file, converted_file, config)
            
        if not(skip_create_metadata):
            CMAR_fetch_metadata.main(dataset_id, config, dataset_directory, final_output_directory)

        if not(skip_create_datasetxml):
            CMAR_generate_xml.main(dataset_id, config, final_output_directory)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("output_directory", type=str,
                    help="Output directory for data and metadata")

    parser.add_argument("config_file", type=str,
                    help="Configuration file for settings")

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
    config_file = args.config_file
    main(
        output_directory,
        config_file,
        args.skip_fetch_data,
        args.skip_split_stations,
        args.skip_merge_data,
        args.skip_create_metadata,
        args.skip_create_datasetxml
    )