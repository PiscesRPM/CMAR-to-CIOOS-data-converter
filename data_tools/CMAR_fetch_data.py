import os
import pandas as pd
from sodapy import Socrata
import sys
import argparse
import yaml
from datetime import datetime
import csv
import gc
from configparser import ConfigParser

"""
dateparse = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
df = pd.read_csv(infile, parse_dates=['datetime'], date_parser=dateparse)
"""

dtype_config_file = os.path.join(os.path.dirname(__file__), '..', 'data_types.yaml')


def join_output_path(base_directory, new_path):
    if not(os.path.exists(base_directory)):
            os.mkdir(base_directory)

    output = os.path.join(
            base_directory,
            new_path
    )
    return output

def split_downloads(dataset_id, output_directory, config, config_section):
    client = Socrata("data.novascotia.ca", 'cx4XloH6tDgX8ZhFFmtAuxtMc')
    client.timeout = 30
    
    dtypes_file = ""
    dtypes_config = {}
    if 'dtype_config_file' in config[config_section]:
        dtypes_file = config[config_section]['dtype_config_file']
        dtype_config_file = os.path.join(os.path.dirname(__file__), '..', dtypes_file)
        if os.path.exists(dtype_config_file):
            with open(dtype_config_file) as f:
                dtypes_config = yaml.load(f, Loader=yaml.FullLoader)
        else:
            print("No datatype config found")
    search_condition = ""

    if 'fetch_separate_station_data' in config[config_section]:
        stations = client.get(dataset_id, select="DISTINCT waterbody, station")

        for item in stations:
            waterbody = item['waterbody'].replace("'", "\'")
            station = item['station'].replace("'", "\'")

            output_subdirectory = join_output_path(output_directory, waterbody.replace(" ", "_").replace("'", "_").replace(".","").lower())
            output_subdirectory = join_output_path(output_subdirectory, station.replace(" ", "_").replace("'", "_").replace(".","").lower())

            output_filename = join_output_path(output_subdirectory, f'{dataset_id}_raw.csv')
            search_condition = f'AND waterbody="{waterbody}" AND station="{station}"'
            fetch_data(dataset_id, output_filename, client, config,config_section, dtypes_config, search_condition)
    else:
        output_filename = join_output_path(output_directory, f'{dataset_id}_raw.csv')
        fetch_data(dataset_id, output_filename, client, config, config_section, dtypes_config)
    client.close()
    return

def fetch_data(dataset_id, output_file, client, config, config_section, dtypes_config, search_condition=''):
    """Fetch data from the open data Nova Scotia portal and write to a CSV

    Args:
        dataset_id (String):    The id of a dataset in the nova scotia open data portal, this is normally found in the url for the dataset 
        output_filename (String): The name of the file where the dataset will be stored

    Returns:
        pandas.DataFrame: A Pandas DataFrame containing the retreived data
    """    


        # Select distinct waterbody, station pairs
        # Create folder for waterbody, station
        # Keep file naming within each station, just add for loop purposes

    # Start the loop here

    # For folder in output folder (waterbody)
    # For folder in waterbody folder (station)
    # Within station folder have a _raw.csv dataset file to look through
    # Filter by waterbody, station before pulling

    #TODO Figure out how this will change the wave methodology - methods!

    sort_order = config[config_section]['dataset_sort_order']
    limit = 1000000
    existing_df=None
    offset=0
    more_data = True
    while more_data:
        results = None
        if offset > 0:
            print("Getting more data from client. Current rows in dataset is " + str(len(existing_df)))
            results = client.get(
                dataset_id, 
                # Figure out this section for wave data
                where=f'1=1 {search_condition}' , 
                order=sort_order, 
                limit=limit, 
                offset=offset)
        elif os.path.exists(output_file):
            with open(output_file, 'r') as f:
                dataset_columns = csv.DictReader(f).fieldnames
                existing_dtypes= {}
                # Check column types from csv data - ignore timestamp as it must be read separately
                for col in dataset_columns:
                    if col in dtypes_config and not dtypes_config[col]=='datetime64[ns]':
                        existing_dtypes[col] = dtypes_config[col]
            existing_df = pd.read_csv(output_file, parse_dates=['timestamp_utc'], dtype=existing_dtypes)
            if 'timestamp_utc' in dtypes_config:
                existing_df = pd.read_csv(output_file, parse_dates=['timestamp_utc'], dtype=existing_dtypes)
            else:
                existing_df = pd.read_csv(output_file, dtype=existing_dtypes)
            offset = len(existing_df)
            results = client.get(
                dataset_id, 
                where=f'1=1 {search_condition}' , 
                order=sort_order, 
                limit=limit, 
                offset=offset)
            
            # Switched to using offsets as some datasets contain data from multiple stations
            #   which may have overlapping timestamp readings that could get cut off
            #max_date_string = max_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
            #results = client.get(dataset_id, where="timestamp_utc > \"%s\"" % max_date_string, order="timestamp_utc", limit=limit)
        else:
            results = client.get(
                dataset_id, 
                where=f'1=1 {search_condition}' , 
                order=sort_order, 
                limit=limit)

        # Convert to pandas DataFrame
        results_df = pd.DataFrame.from_records(results)
        new_rows = len(results_df)
        print(str(new_rows) + " new rows found")

        # Some fields from API will end with _, use below to strip
        results_df = results_df.rename(columns=lambda x: x.strip('_'))
        
        # Output the unmerged data if desired
        if new_rows > 0:
            dtypes= {}
            # Headers can change depending on whether the variables exist in the data section
            # Need to recheck every query to be safe
            for col in results_df.columns:
                if col in dtypes_config:
                    dtypes[col] = dtypes_config[col]
            results_df = results_df.astype(dtype=dtypes)

            print("Found new data, writing to raw file: %s" % output_file)
            if existing_df is not None:
                results_df = existing_df.append(results_df).astype(dtype=dtypes)

            results_df.to_csv(output_file, index=False)
            if new_rows < limit:
                print("New data successfully retrieved")
                more_data = False
            else:
                print("Data limit hit. Calling client again")
                offset = len(results_df)
                existing_df = results_df
        else:
            print("No new data found, proceeding with data from: %s" % output_file)
            more_data = False
            results_df = existing_df
        del results_df
        gc.collect()

    # Help clear up some space
    del existing_df
    gc.collect()
    return 

def main(dataset_id, output_directory, config, config_section='main'):
    split_downloads(dataset_id, output_directory, config, config_section)
    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("dataset_id", type=str,
                    help="Dataset ID")
    parser.add_argument("output_filename", help="custom name of output file")
    args = parser.parse_args()

    dataset_id = args.dataset_id
    output_filename = args.output_filename

    main(dataset_id, output_filename)