# Import libraries
import os
import pandas as pd
import sys

def main():
    
    args = sys.argv[1:]
    if len(args) == 2 and args[0] == '-f':
        OGfile = args[1]
        if (OGfile[-4:] != ".csv"):
            print("File must have a csv extension. \nExample:python CSV_to_Unique_Stations.py -f data.csv")
            exit(0)
    else:
        print("Please input a -f followed with a csv file. \nExample:python CSV_to_Unique_Stations.py -f data.csv")
        exit(0)
    df = pd.read_csv(OGfile,parse_dates=['timestamp'])
    original = os.path.splitext(OGfile)[0]
    stations = df["station"].unique()
    #"eb3n-uxcb_raw.csv"
    for station in stations:
        binary_filter = df["station"] == station
        data_for_current_station = df[binary_filter]
        file_name = original + "_"+ station + ".csv"
        file_name = file_name.replace(" ", "_")
        data_for_current_station.to_csv(file_name,index=False)
if __name__ == "__main__":
    main()