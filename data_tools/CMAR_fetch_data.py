import os
import pandas as pd
from sodapy import Socrata
import sys
import argparse

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

def main(dataset_id=None, output_raw_csv=False, raw_csv_filename=None):
    df = fetch_data(dataset_id, output_raw_csv, raw_csv_filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("SetID", type=str,
                    help="Dataset ID")
    parser.add_argument("-o", help="custom name of output file")
    args = parser.parse_args()
    if args.o:
        output_raw_csv = args.o
        print("Custom name %s" %args.o)
    else:
        output_raw_csv = None
    dataset_id = args.SetID

    main(dataset_id=dataset_id, output_raw_csv=True, raw_csv_filename=output_raw_csv)