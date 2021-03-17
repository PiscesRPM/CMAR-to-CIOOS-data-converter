# Import libraries
import os
import pandas as pd
from sodapy import Socrata

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
        ['waterbody', 'station', 'lease', 'latitude', 
            'longitude', 'deployment_period', 'timestamp', 
            'sensor', 'depth'
        ], as_index=False
    )

    merged_df = grouped_df.apply(merge_timestamps)
    return merged_df

def main(dataset_id="eb3n-uxcb", merged_csv_filename=None, output_raw_csv=False, raw_csv_filename=None):
    df = fetch_data(dataset_id, output_raw_csv, raw_csv_filename)

    # Merge data based on waterbody, station, lease, latitude, longitude, deployment_period, timestamp, and sensor 
    merged_df = group_by_timestamp(df)
    if merged_csv_filename is None:
        merged_csv_filename = "%s_merged.csv" % dataset_id
    merged_df.to_csv(merged_csv_filename, index=False)

if __name__ == "__main__":
    main(output_raw_csv=True)