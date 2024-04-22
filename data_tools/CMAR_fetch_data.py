import os
import pandas as pd
from sodapy import Socrata
import sys
import argparse

def fetch_data(dataset_id, output_filename):
    """Fetch data from the open data Nova Scotia portal and write to a CSV

    Args:
        dataset_id (String):    The id of a dataset in the nova scotia open data portal, this is normally found in the url for the dataset 
        output_filename (String): The name of the file where the dataset will be stored

    Returns:
        pandas.DataFrame: A Pandas DataFrame containing the retreived data
    """    
    limit = 60000000
    client = Socrata("data.novascotia.ca", 'cx4XloH6tDgX8ZhFFmtAuxtMc')

    results = None
    existing_df = None
    if os.path.exists(output_filename):
        existing_df = pd.read_csv(output_filename, parse_dates=['timestamp_utc'])
        max_date = existing_df['timestamp_utc'].max()
        max_date_string = max_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
        results = client.get(dataset_id, where="timestamp_utc > \"%s\"" % max_date_string, limit=limit)
    else:
        results = client.get(dataset_id, limit=limit)

    # Convert to pandas DataFrame
    results_df = pd.DataFrame.from_records(results)

    # Some fields from API will end with _, use below to strip
    results_df = results_df.rename(columns=lambda x: x.strip('_'))
    
    new_rows = len(results_df)
    if existing_df is not None:
        results_df = results_df.append(existing_df)

    # Output the unmerged data if desired
    if new_rows > 0:
        print("Found new data, writing to raw file: %s" % output_filename)

        results_df.to_csv(output_filename, index=False)

    if new_rows == 0:
        print("No new data found, proceeding with data from: %s" % output_filename)
    return results_df

def main(dataset_id, output_filename):
    df = fetch_data(dataset_id, output_filename)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("dataset_id", type=str,
                    help="Dataset ID")
    parser.add_argument("output_filename", help="custom name of output file")
    args = parser.parse_args()

    dataset_id = args.dataset_id
    output_filename = args.output_filename

    main(dataset_id, output_filename)