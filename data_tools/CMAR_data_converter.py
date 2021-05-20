# Import libraries
import os
import pandas as pd
from sodapy import Socrata
from tqdm import tqdm
import sys
import argparse

from . import util

def merge_timestamps(grouped_df):
    non_unique = ['variable', 'value']
    
    new_df = pd.DataFrame()
    
    # Copy over all of the unique columns     
    for col in grouped_df:
        if col not in non_unique:
            if col == 'deployment_period':
                deployment_dates = grouped_df[col].unique()[0].split(' to ',1)
                new_df['deployment_start_date'] = deployment_dates[0]
                new_df['deployment_end_date'] = deployment_dates[1]
            else:
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

def setup_merged_output_filename(input_filename, output_directory):
    original_filename = os.path.splitext(os.path.basename(input_filename))[0]

    merged_output_filename = os.path.join(
        output_directory,
        "%s_merged.csv" % (original_filename)
    )
    return merged_output_filename

def main(input_filename, output_directory):
    util.check_raw_file_extension(input_filename)
    
    # df = fetch_data(dataset_id, output_raw_csv, raw_csv_filename)
    df = pd.read_csv(input_filename)
    
    merged_output_filename = setup_merged_output_filename(input_filename, output_directory)
    # Merge data based on waterbody, station, lease, latitude, longitude, deployment_period, timestamp, and sensor 
    merged_df = group_by_timestamp(df)
    merged_df.to_csv(merged_output_filename, index=False)
    return merged_output_filename
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str,
                    help="Input file name")
    parser.add_argument("output_directory", help="custom name of output file")
    args = parser.parse_args()

    input_filename = args.input
    output_directory = args.output_directory
    main(input_filename, output_directory)