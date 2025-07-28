import os
import argparse
import glob
import yaml
import pandas as pd
from configparser import ConfigParser

from data_tools import CMAR_fetch_data, CSV_split_by_station, CMAR_data_converter, CMAR_fetch_metadata, CMAR_generate_xml

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
        reference_dataset_id = config['reference']['reference_dataset_id']
        reference_dtype_file = config['reference']['dtype_config_file']
        reference_dataset_filename = os.path.join(output_directory, reference_dataset_id) + "_raw.csv"
        if not(skip_fetch_data):
            CMAR_fetch_data.main(reference_dataset_id, reference_dataset_filename, config, 'reference')
        reference_dataset = pd.read_csv(reference_dataset_filename, dtype={config['metadata']['col_instrument_id']: 'category'})

    dataset_id_list = config['main']['dataset_id_list'].splitlines()
    for dataset_id in dataset_id_list:
        # Create a new folder inside of output_directory for each dataset
        dataset_directory = os.path.join(output_raw_directory, dataset_id)
        dataset_output_directory = os.path.join(dataset_directory, "data_by_station")

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

        raw_dataset_files = get_matching_files(dataset_output_directory, "*_raw.csv")
        
        # If there's a reference dataset, join with the downloaded dataset
        merged_output_directory = os.path.join(dataset_output_directory, "final_output")
        output_filename = raw_output_filename
        if reference_dataset is not None:
            if not reference_dataset.empty:
                reference_dataset_columns = config['reference']['dataset_columns'].splitlines()
                dataset= pd.read_csv(raw_output_filename)
                joined_df = CMAR_join_datasets.main(dataset, reference_dataset, config['reference']['join_key'], reference_dataset_columns)
                joined_output_filename = setup_output_filename(merged_output_directory,dataset_id,'joined')
                joined_df.to_csv(joined_output_filename, index=False)
                print(joined_output_filename)

        # TODO: Fix this to work for going through subdirectories
        if len(raw_dataset_files) < 1:
            raise Exception("DatasetID: %s returned no dataset. Make sure the datasetID exists in data.novascotia.ca" % dataset_id)

        # Already does this during the download, can probably cut out
        if not(skip_split_stations):
            station_output_directory = os.path.join(dataset_output_directory, "data_by_station")
            if not(os.path.exists(station_output_directory)):
                os.mkdir(station_output_directory)
            CSV_split_by_station.main(output_filename, station_output_directory)


        # Iterate through every CSV file in station_output_directory and merge
        # Actually don't merge?
        # TODO this will need to be adapted to new changes
        if not(skip_merge_data):
            if not(skip_split_stations):
                for input_filename in glob.glob(os.path.join(station_output_directory, "*.csv")):
                    CMAR_data_converter.main(input_filename, merged_output_directory)
            else:
                CMAR_data_converter.main(raw_output_filename, merged_output_directory, config)
        else:
            for raw_file in raw_dataset_files:
                converted_file = raw_file.replace("_raw", "_final")
                os.makedirs(os.path.dirname(converted_file), exist_ok=True)
                CMAR_data_converter.main(raw_file, converted_file, config)
            
        final_output_directory = dataset_directory.replace("/raw/", "/erddap/")
        if not(skip_create_metadata):
            CMAR_fetch_metadata.main(dataset_id, config, dataset_output_directory, final_output_directory)

        if not(skip_create_datasetxml):
            #for data_file in glob.glob(os.path.join(merged_output_directory, "*.csv")):
            CMAR_generate_xml.main(dataset_id, config, dataset_output_directory, final_output_directory)

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