import os
import pandas as pd
from sodapy import Socrata
import sys
import argparse
from pathlib import Path

from data_tools import CMAR_fetch_data


def main():
    # example list
    dataset_id_list = ['eb3n-uxcb','x9dy-aai9']
    for set_id in dataset_id_list:
        # os.system("CMAR_fetchData.py ")
        # CMAR_fetch_data.main() <--- call like this
        outputFolder = os.path.dirname(__file__) + '/' + set_id + ' Data Set'
        path = os.path.join(os.path.dirname(__file__), outputFolder)
        # Path(path).mkdir(parents=True, exist_ok=True)
        if not os.path.exists(path):
            os.mkdir(path)


if __name__ == "__main__":
    main()