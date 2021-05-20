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

def main(OGfile=None, merged_csv_filename=None, output_raw_csv=False, raw_csv_filename=None):
    if (OGfile[-4:] != ".csv"):
        print("File must have a csv extension. \nExample:python CSV_to_Unique_Stations.py -f data.csv")
        exit(0)
    
    # df = fetch_data(dataset_id, output_raw_csv, raw_csv_filename)
    df = pd.read_csv(OGfile)
    # Merge data based on waterbody, station, lease, latitude, longitude, deployment_period, timestamp, and sensor 
    merged_df = group_by_timestamp(df)
    if ('data_tools') in OGfile:
        # OGfile = OGfile.split('\\')[2] #use when using command line arguments
        OGfile = OGfile.split('/')[3]
    if merged_csv_filename is None:
        merged_csv_filename = "%s_merged.csv" % OGfile.split(".")[0]    
    if merged_csv_filename != None:
        outputFolder2 = None
        outputFolder2 = os.path.dirname(__file__) + '/' + OGfile.split("_raw")[0] + '/' + OGfile.split(".")[0]
        print(outputFolder2,"THIS IS OUTPUTFOLDER")
        path = os.path.join(os.path.dirname(__file__), outputFolder2)
        if not os.path.exists(path):
            os.mkdir(path)
        merged_df.to_csv(os.path.join(path,merged_csv_filename), index=False)
    else:
        merged_df.to_csv(merged_csv_filename, index=False)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str,
                    help="Input file name")
    parser.add_argument("-o", help="custom name of output file")
    args = parser.parse_args()
    if args.o:
        merged_csv_filename = args.o
        print("Custom name %s" %args.o)
    OGfile = args.input
    main(output_raw_csv=True)