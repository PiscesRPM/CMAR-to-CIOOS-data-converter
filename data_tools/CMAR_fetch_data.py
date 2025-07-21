import os
import pandas as pd
from sodapy import Socrata
import sys
import argparse
import yaml
from datetime import datetime
import csv
import gc

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

def fetch_data(dataset_id, output_directory, output_file_format):
    """Fetch data from the open data Nova Scotia portal and write to a CSV

    Args:
        dataset_id (String):    The id of a dataset in the nova scotia open data portal, this is normally found in the url for the dataset 
        output_filename (String): The name of the file where the dataset will be stored

    Returns:
        pandas.DataFrame: A Pandas DataFrame containing the retreived data
    """    
    limit = 1000000
    client = Socrata("data.novascotia.ca", 'cx4XloH6tDgX8ZhFFmtAuxtMc')
    client.timeout = 30

    dtypes_config = {}
    if os.path.exists(dtype_config_file):
        with open(dtype_config_file) as f:
            dtypes_config = yaml.load(f, Loader=yaml.FullLoader)
    else:
        print("No datatype config found")

        # Select distinct waterbody, station pairs
        # Create folder for waterbody, station
        # Keep file naming within each station, just add for loop purposes

    # Start the loop here

    # For folder in output folder (waterbody)
    # For folder in waterbody folder (station)
    # Within station folder have a _raw.csv dataset file to look through
    # Filter by waterbody, station before pulling

    #TODO Figure out how this will change the wave methodology - methods!

    stations = client.get(dataset_id, select="DISTINCT waterbody, station")

    for item in stations:
        waterbody = item['waterbody'].replace("'", "\'")
        station = item['station'].replace("'", "\'")

        output_subdirectory = join_output_path(output_directory, waterbody.replace(" ", "_").replace("'", "_").replace(".","").lower())
        output_subdirectory = join_output_path(output_subdirectory, station.replace(" ", "_").replace("'", "_").replace(".","").lower())

        output_filename = join_output_path(output_subdirectory, f'{dataset_id}_raw.csv')

        existing_df=None
        offset=0
        more_data = True
        while more_data:
            results = None
            if offset > 0:
                print("Getting more data from client. Current rows in dataset is " + str(len(existing_df)))
                results = client.get(
                    dataset_id, 
                    where=f'waterbody="{waterbody}" AND station="{station}"' , 
                    order="timestamp_utc,sensor_type,sensor_serial_number", 
                    limit=limit, 
                    offset=offset)
            elif os.path.exists(output_filename):
                with open(output_filename, 'r') as f:
                    dataset_columns = csv.DictReader(f).fieldnames
                    existing_dtypes= {}
                    # Check column types from csv data - ignore timestamp as it must be read separately
                    for col in dataset_columns:
                        if col in dtypes_config and not dtypes_config[col]=='datetime64[ns]':
                            existing_dtypes[col] = dtypes_config[col]
                existing_df = pd.read_csv(output_filename, parse_dates=['timestamp_utc'], dtype=existing_dtypes)
                offset = len(existing_df)
                print(offset)
                results = client.get(
                    dataset_id, 
                    where=f'waterbody="{waterbody}" AND station="{station}"' , 
                    order="timestamp_utc,sensor_type,sensor_serial_number", 
                    limit=limit, 
                    offset=offset)
                
                # Switched to using offsets as some datasets contain data from multiple stations
                #   which may have overlapping timestamp readings that could get cut off
                #max_date_string = max_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
                #results = client.get(dataset_id, where="timestamp_utc > \"%s\"" % max_date_string, order="timestamp_utc", limit=limit)
            else:
                results = client.get(
                    dataset_id, 
                    where=f'waterbody="{waterbody}" AND station="{station}"' , 
                    order="timestamp_utc,sensor_type,sensor_serial_number", 
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

                print("Found new data, writing to raw file: %s" % output_filename)
                if existing_df is not None:
                    results_df = existing_df.append(results_df).astype(dtype=dtypes)

                results_df.to_csv(output_filename, index=False)
                if new_rows < limit:
                    print("New data successfully retrieved")
                    more_data = False
                else:
                    print("Data limit hit. Calling client again")
                    offset = len(results_df)
                    existing_df = results_df
            else:
                print("No new data found, proceeding with data from: %s" % output_filename)
                more_data = False
                results_df = existing_df
            del results_df
            gc.collect()

    # Help clear up some space
    del existing_df
    gc.collect()
    client.close()
    return 

def main(dataset_id, output_directory, output_file_format):
    fetch_data(dataset_id, output_directory, output_file_format)
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