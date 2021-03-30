import os
import pandas as pd
from sodapy import Socrata
import sys


def fetch_data(dataset_id, output_raw_csv=False, raw_csv_filename=None):
    
    
    limit = 6000000
    client = Socrata("data.novascotia.ca", 'cx4XloH6tDgX8ZhFFmtAuxtMc')

    if raw_csv_filename is None:
        raw_csv_filename = "%s_raw.csv" % dataset_id

    results = None
    existing_df = None
    if os.path.exists(raw_csv_filename):
        existing_df = pd.read_csv(raw_csv_filename, parse_dates=['timestamp'])
        max_date = existing_df['timestamp'].max()
        max_date_string = max_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
        results = client.get(dataset_id, where="timestamp > \"%s\"" % max_date_string, limit=limit)
    else:
        results = client.get(dataset_id, limit=limit)
    # results = client.get("eb3n-uxcb", limit=500000)


    # Convert to pandas DataFrame
    results_df = pd.DataFrame.from_records(results)

    new_rows = len(results_df)
    if existing_df is not None:
        results_df = results_df.append(existing_df)

    # Output the unmerged data if desired
    if output_raw_csv and new_rows > 0:
        print("Found new data, writing to raw file: %s" % raw_csv_filename)
        results_df.to_csv(raw_csv_filename, index=False)

    if new_rows == 0:
        print("No new data found, proceeding with data from: %s" % raw_csv_filename)
    return results_df

def main(dataset_id="eb3n-uxcb", output_raw_csv=False, raw_csv_filename=None):
    args = sys.argv[1:]
    if len(args) == 2 and args[0] == '-setID':
        dataset_id = args[1]
    else:
        print("Missing arguments.\nPlease input a -setID followed with the dataset ID. \nExample:python CSV_to_Unique_Stations.py -setID 1234")
        exit(0)
    
    df = fetch_data(dataset_id, output_raw_csv, raw_csv_filename)


if __name__ == "__main__":
    main(output_raw_csv=True)