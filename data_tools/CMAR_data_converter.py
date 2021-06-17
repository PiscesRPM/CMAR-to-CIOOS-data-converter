# Import libraries
import os
import pandas as pd
from sodapy import Socrata
from tqdm import tqdm
import sys
import argparse

from . import util


def group_by_timestamp(df):
    pivot_index = ['waterbody', 'station', 'latitude', 'longitude', 
        'deployment_period', 'timestamp', 'sensor', 'depth']
    merged_df = df.pivot(index=pivot_index, columns='variable', values='value').reset_index()
    return merged_df

def split_deployment_period(df):
    new_df = pd.DataFrame()

    for col in df:
        if col == 'deployment_period':
            deployment_dates = df[col].unique()[0].split(' to ',1)
            new_df['deployment_start_date'] = deployment_dates[0]
            new_df['deployment_end_date'] = deployment_dates[1]
        else:
            new_df[col] = df[col]
    return new_df

def setup_merged_output_filename(input_filename, output_directory):
    original_filename = os.path.splitext(os.path.basename(input_filename))[0]

    merged_output_filename = os.path.join(
        output_directory,
        "%s_merged.csv" % (original_filename)
    )
    return merged_output_filename

def main(input_filename, output_directory):
    util.check_raw_file_extension(input_filename)
    
    df = pd.read_csv(input_filename)
    
    merged_output_filename = setup_merged_output_filename(input_filename, output_directory)

    # Merge data based on waterbody, station, lease, latitude, longitude, deployment_period, timestamp, and sensor 
    merged_df = group_by_timestamp(df)
    merged_df = split_deployment_period(merged_df)
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