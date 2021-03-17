# Import libraries
import pandas as pd
from sodapy import Socrata

def fetch_data(dataset_id):
    client = Socrata("data.novascotia.ca", 'cx4XloH6tDgX8ZhFFmtAuxtMc')
   
    results = client.get(dataset_id, limit=6000000)
    # results = client.get("eb3n-uxcb", limit=500000)

    # Convert to pandas DataFrame
    results_df = pd.DataFrame.from_records(results)
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
        if len(grouped_df['value'].unique()) > 1:
            raise(ValueError("Multiple values for the same variable at timestamp: %s" % new_df['timestamp']))
        else:
            new_df[variable] = grouped_df['value'].unique()
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
    df = fetch_data(dataset_id)
    # Output the unmerged data if desired
    if output_raw_csv:
        if raw_csv_filename is None:
            raw_csv_filename = "%s_raw.csv" % dataset_id
        df.to_csv(raw_csv_filename, index=False)

    # Merge data based on waterbody, station, lease, latitude, longitude, deployment_period, timestamp, and sensor 
    merged_df = group_by_timestamp(df)
    if merged_csv_filename is None:
        merged_csv_filename = "%s_merged.csv" % dataset_id
    merged_df.to_csv(merged_csv_filename, index=False)

if __name__ == "__main__":
    main(output_raw_csv=True)