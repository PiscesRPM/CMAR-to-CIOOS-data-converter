# Import libraries
import os
import pandas as pd
import sys
import argparse
from data_tools import CMAR_data_converter, CMAR_fetch_metadata
def main(OGfile, outputFolder = None):
    if (OGfile[-4:] != ".csv"):
        print("File must have a csv extension. \nExample:python CSV_to_Unique_Stations.py -f data.csv")
        exit(0)
    df = pd.read_csv(OGfile,parse_dates=['timestamp'])
    original = os.path.splitext(OGfile)[0]
    if ('data_tools') in original:
        original = original.split('%s\\' %outputFolder, 1)[1]
    stations = df["station"].unique()
    #"eb3n-uxcb_raw.csv"
    if outputFolder != None:
            outputFolder2 = os.path.dirname(__file__) + '/' + outputFolder
            path = os.path.join(os.path.dirname(__file__), outputFolder2)
            if not os.path.exists(path):
                os.mkdir(path)
    for station in stations:
        file_name = None
        binary_filter = df["station"] == station
        data_for_current_station = df[binary_filter]
        file_name = original + "_"+ station + ".csv"
        file_name = file_name.replace(" ", "_")
            # yamlName = os.path.join(outputFolder,yamlName)
        if outputFolder != None:
            outputFolder2 = None
            outputFolder2 = os.path.dirname(__file__) + '/' + outputFolder + '/' + file_name
            path = os.path.join(os.path.dirname(__file__), outputFolder2[:-4])
            if not os.path.exists(path):
                os.mkdir(path)
            data_for_current_station.to_csv(os.path.join(path,file_name),index=False)
            split_station_file = path + '/' + file_name
            print(split_station_file,"THIS IS SPLIT_FILE_NAME IMPORTANT")
            merged_station_file = path + '/' + file_name.replace('.csv','_merged.csv')
            meta_file_folder = outputFolder + '/' + file_name[:-4]
            CMAR_data_converter.main(split_station_file)
            CMAR_fetch_metadata.main(outputFolder,merged_station_file,meta_file_folder)
        else:
            data_for_current_station.to_csv(file_name,index=False)
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # args = sys.argv[1:]
    parser.add_argument("input", type=str,
                    help="Input file name")
    parser.add_argument("-o", help="custom name of output directory")
    args = parser.parse_args()
    input_file = args.input
    outputFolder = None
    if args.o:
        outputFolder = args.o
        print("Files will be outputted in %s" %args.o)


    main(input_file, outputFolder)