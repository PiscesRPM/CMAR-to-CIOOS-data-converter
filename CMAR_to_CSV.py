# Import libraries
import os
import pandas as pd
from sodapy import Socrata
from tqdm import tqdm
import sys
import argparse

def merge_timestamps(grouped_df):
    non_unique = ['variable', 'value']
    
    new_df = pd.DataFrame()
    
    # Copy over all of the unique columns     
    for col in grouped_df:
        if col not in non_unique:
            new_df[col] = grouped_df[col].unique()
    
    # Handle the non-unique columns
    # Create a column for each variable, store the associated value
    # If there are multiple, different values for the same variable, at the same time, raise an error     
    sci_variable_list = grouped_df['variable'].unique()
    for variable in sci_variable_list:
        rows_for_variable = grouped_df[grouped_df['variable'] == variable]
        if len(rows_for_variable['value'].unique()) > 1:
            raise(ValueError("Multiple values for the same variable at timestamp: %s" % new_df['timestamp']))
        else:
            new_df[variable] = rows_for_variable['value'].unique()
    return new_df

def group_by_timestamp(df):
    grouped_df = df.groupby(
        ['waterbody', 'station', 'latitude', 
            'longitude', 'deployment_period', 'timestamp', 
            'sensor', 'depth'
        ], as_index=False
    )
    tqdm.pandas()
    merged_df = grouped_df.progress_apply(merge_timestamps)
    return merged_df

def main(dataset_id="eb3n-uxcb", merged_csv_filename=None, output_raw_csv=False, raw_csv_filename=None):
    parser = argparse.ArgumentParser()
    # args = sys.argv[1:]
    parser.add_argument("input", type=str,
                    help="Input file name")
    parser.add_argument("-o", help="custom name of output file")
    args = parser.parse_args()
    if args.o:
        merged_csv_filename = args.o
        print("Custom name %s" %args.o)
    OGfile = args.input
    if (OGfile[-4:] != ".csv"):
        print("File must have a csv extension. \nExample:python CSV_to_Unique_Stations.py -f data.csv")
        exit(0)
    
    # df = fetch_data(dataset_id, output_raw_csv, raw_csv_filename)
    df = pd.read_csv(OGfile)
    # Merge data based on waterbody, station, lease, latitude, longitude, deployment_period, timestamp, and sensor 
    merged_df = group_by_timestamp(df)
    if merged_csv_filename is None:
        merged_csv_filename = "%s_merged.csv" % OGfile
    merged_df.to_csv(merged_csv_filename, index=False)

if __name__ == "__main__":
    main(output_raw_csv=True)