# Import libraries
import os
import pandas as pd
from sodapy import Socrata
from tqdm import tqdm
import sys
import argparse
import yaml

from . import util

qualitative_values_config_file = 'qualitative_to_quantitative.yaml'

def group_by_timestamp(df):
    pivot_index = ['waterbody', 'station', 'lease', 'latitude', 'longitude', 
        'deployment_period', 'timestamp', 'sensor', 'depth']
    merged_df = df.pivot(index=pivot_index, columns='variable', values='value').reset_index()
    return merged_df

def qualitative_to_quantitative(df):
    df['depth'] = df['depth'].apply(pd.to_numeric, errors='ignore')
    depths = df['depth'].unique()
    
    string_depths = []
    for depth in depths:
        if (isinstance(depth,str)):
            string_depths.append(depth)

    if os.path.exists(qualitative_values_config_file):
            with open(qualitative_values_config_file) as f:
                qualitative_values_config = yaml.load(f, Loader=yaml.FullLoader)
    else:
        qualitative_values_config = {}

    stations = df['station'].unique()
    for station in stations:
        for depth in string_depths:
            selector = (df['station'] == station) & (df['depth'] == depth)
            affected_rows = df[selector]
            if len(affected_rows) > 0:
                if (station not in  qualitative_values_config):
                    print("THIS IS THE DEPTH:",depth)
                    raise Exception("Station: %s not found in qualitative values config: %s" % (station, qualitative_values_config_file))
                else:
                    station_config = qualitative_values_config[station]
                    if (depth not in  station_config):
                        raise Exception("Depth value: %s not found in qualitative values config: %s for station: %s" % (
                            depth,
                            qualitative_values_config_file,
                            station
                        ))
                    else:
                        df.loc[selector,('depth',)] = float(station_config[depth])
    return df

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

def group_waterbody_station(merged_df):
    merged_df['waterbody_station'] = merged_df['waterbody'] + '-' + merged_df['station']
    merged_df = merged_df.drop(columns=['waterbody', 'station'])
    cols = merged_df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    merged_df = merged_df[cols]

    return merged_df

def main(input_filename, output_directory):
    util.check_raw_file_extension(input_filename)
    
    df = pd.read_csv(input_filename)
    
    merged_output_filename = setup_merged_output_filename(input_filename, output_directory)

    # Merge data based on waterbody, station, lease, latitude, longitude, deployment_period, timestamp, and sensor 
    merged_df = group_by_timestamp(df)
    merged_df = split_deployment_period(merged_df)
    merged_df = qualitative_to_quantitative(merged_df)
    merged_df = group_waterbody_station(merged_df)
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