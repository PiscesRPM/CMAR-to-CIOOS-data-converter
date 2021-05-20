import os
import pandas as pd
from sodapy import Socrata
import sys
import argparse
from pathlib import Path

from data_tools import CMAR_fetch_data, CSV_split_by_station, CMAR_data_converter, CMAR_fetch_metadata


def main():
    # example list
    dataset_id_list = ['eb3n-uxcb','x9dy-aai9']
    for set_id in dataset_id_list:
        # CMAR_fetch_data.main(set_id, True)
        raw_file_name = 'data_tools\%s\%s' % (set_id,set_id + '_raw.csv')
        CSV_split_by_station.main(raw_file_name,set_id)



if __name__ == "__main__":
    main()