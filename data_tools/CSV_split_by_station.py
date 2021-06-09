# Import libraries
import os
import pandas as pd
import sys
import argparse

from . import util


def get_data_for_station(dataframe, station_name):
    """Filters the data in a dataframe to get the data for a particular station

    Args:
        dataframe (pandas.DataFrame): The DataFrame that data should be split out of
        station_name (String): The name of the station to be extracted

    Returns:
        pandas.DataFrame: A DataFrame containing data for just one station
    """    
    binary_filter = dataframe["station"] == station_name
    data_for_current_station = dataframe[binary_filter]
    return data_for_current_station

def setup_station_output_filename(raw_filename, output_directory, station_name):
    """Builds a filename for a split file including path and creates directories as needed

    Args:
        raw_filename (String): The name of the raw file that the split file is being created from
        output_directory (String): The output directory where the split data will be stored
        station_name (String): The name of the current station

    Returns:
        String: The filename including path for the split file
    """    
    original_filename = os.path.splitext(os.path.basename(raw_filename))[0]


    station_output_filename = os.path.join(
        output_directory,
        "%s_%s.csv" % (original_filename, station_name)
    )
    return station_output_filename

def main(raw_filename, output_directory):
    util.check_raw_file_extension(raw_filename)

    df = pd.read_csv(raw_filename, parse_dates=['timestamp'])


    stations = df["station"].unique()

    station_files = []

    for station in stations:
        # Get the data for the current station
        data_for_current_station = get_data_for_station(df, station)
        # Figure out a filename for the station
        station_output_filename = setup_station_output_filename(raw_filename, output_directory, station)
        # store the data for the station
        data_for_current_station.to_csv(station_output_filename, index=False)
        station_files.append(station_output_filename)
    return station_files

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # args = sys.argv[1:]
    parser.add_argument("input", type=str,
                    help="Input file name")
    parser.add_argument("output_directory", help="Name of output directory")
    args = parser.parse_args()
    input_file = args.input
    output_directory = args.output_directory

    main(input_file, output_directory)