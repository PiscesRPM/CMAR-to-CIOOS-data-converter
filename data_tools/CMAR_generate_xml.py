import requests
import argparse
import os
import yaml
from datetime import datetime
import uuid
import pandas as pd
from xml.etree.ElementTree import ElementTree, tostring
import xml.etree.cElementTree as ET

variable_config_file = 'variables.yaml'

url = 'https://data.novascotia.ca/api/views/metadata/v1/x9dy-aai9'
def get_metadata(dataset_id):
    url = 'https://data.novascotia.ca/api/views/metadata/v1/'
    url = url + dataset_id
    return requests.get(url).json()

def get_variables(dataset_id):
    url = 'https://data.novascotia.ca/api/catalog/v1?ids='
    url = url + dataset_id
    return requests.get(url).json()

def generate_from_metadata(dataset_id, df, data_file):
    metadata = get_metadata(dataset_id)
    description = get_variables(dataset_id)
    instruments = get_instruments(df)
    instruments = ','.join(instruments)


    column_names = list(df.columns.values)

    lat_lon_spatial = get_bbox(df)
    vertical_spatial = get_vertical(df)

    title = metadata['name'] #title
    date_created = metadata['createdAt'] #creattion + publication
    summary = metadata['customFields']['Detailed Metadata']['Usage Considerations'] #abstract:
    keywords = metadata['tags'] #keywords
    keywords= ",".join(keywords)
    publisher_name = "Centre for Marine Applied Research (CMAR)"
    publisher_email = "info(at)cmar.ca"
    license = metadata['license']
    creator_name = 'CMAR'

    # if (license != 'Nova Scotia Open Government Licence'):
    #     raise Exception('License is not Nova Scotia Open Government License. \t Found License:%s' % license)



    dataset = ET.Element("dataset", active = "true", datasetID=str(dataset_id), type="EDDTableFromAsciiFiles")
    ET.SubElement(dataset, "reloadEveryNMinutes").text = "10080"
    ET.SubElement(dataset, "updateEveryNMillis").text = "10000"
    ET.SubElement(dataset, "fileDir").text = "/datasets/cmar/"
    ET.SubElement(dataset, "fileNameRegex").text = ".*merged\.csv"
    ET.SubElement(dataset, "recursive").text = "true"
    ET.SubElement(dataset, "pathRegex").text = ".*"
    ET.SubElement(dataset, "metadataFrom").text = "last"
    ET.SubElement(dataset, "standardizeWhat").text = "0"
    ET.SubElement(dataset, "charset").text = "ISO-8859-1"
    ET.SubElement(dataset, "columnSeparator").text = ","
    ET.SubElement(dataset, "columnNamesRow").text = "1"
    ET.SubElement(dataset, "firstDataRow").text = "2"
    ET.SubElement(dataset, "sortedColumnSourceName").text = "timestamp"
    ET.SubElement(dataset, "sortFilesBySourceNames").text = "timestamp"
    ET.SubElement(dataset, "fileTableInMemory").text = "false"
    ET.SubElement(dataset, "accessibleViaFiles").text = "true"

    addAttributes = ET.SubElement(dataset, "addAttributes")
    ET.SubElement(addAttributes, "att", name = "cdm_data_type").text = "Point"
    ET.SubElement(addAttributes, "att", name = "Conventions").text = "COARDS, CF-1.6, ACDD-1.3"
    ET.SubElement(addAttributes, "att", name = "creator_name").text = creator_name
    ET.SubElement(addAttributes, "att", name = "creator_type").text = 'insitution'
    ET.SubElement(addAttributes, "att", name = "infoUrl").text = '???'
    ET.SubElement(addAttributes, "att", name = "institution").text = creator_name
    ET.SubElement(addAttributes, "att", name = "license").text = license
    ET.SubElement(addAttributes, "att", name = "sourceUrl").text = "(local files)"
    ET.SubElement(addAttributes, "att", name = "standard_name_vocabulary").text = "CF Standard Name Table v55" 
    ET.SubElement(addAttributes, "att", name = "subsetVariables").text = "waterbody, station, lease, sensor" #ASK
    ET.SubElement(addAttributes, "att", name = "contributor_name").text = publisher_name
    ET.SubElement(addAttributes, "att", name = "contributor_role").text = "owner"
    ET.SubElement(addAttributes, "att", name = "creator_email").text = publisher_email
    ET.SubElement(addAttributes, "att", name = "creator_type").text = "person"
    ET.SubElement(addAttributes, "att", name = "date_created").text = date_created
    ET.SubElement(addAttributes, "att", name = "geospatial_lat_max").text = str(lat_lon_spatial[0])
    ET.SubElement(addAttributes, "att", name = "geospatial_lat_min").text = str(lat_lon_spatial[1])
    ET.SubElement(addAttributes, "att", name = "geospatial_lon_max").text = str(lat_lon_spatial[2])
    ET.SubElement(addAttributes, "att", name = "geospatial_lon_min").text = str(lat_lon_spatial[3])
    ET.SubElement(addAttributes, "att", name = "geospatial_vertical_max").text = str(vertical_spatial[0])
    ET.SubElement(addAttributes, "att", name = "geospatial_vertical_min").text = str(vertical_spatial[1])
    ET.SubElement(addAttributes, "att", name = "institution").text = publisher_name
    ET.SubElement(addAttributes, "att", name = "instrument").text = instruments
    ET.SubElement(addAttributes, "att", name = "keywords").text = keywords
    ET.SubElement(addAttributes, "att", name = "publisher_email").text = publisher_email
    ET.SubElement(addAttributes, "att", name = "publisher_institution").text = publisher_name
    ET.SubElement(addAttributes, "att", name = "publisher_name").text = publisher_name
    ET.SubElement(addAttributes, "att", name = "summary").text = summary
    ET.SubElement(addAttributes, "att", name = "title").text = title
    
    
    fields_names = description['results'][0]['resource']['columns_field_name']
    datatypes = description['results'][0]['resource']['columns_datatype']
    variable_descriptions = description['results'][0]['resource']['columns_description']
    variable_list = fields_names
    for i in range(len(fields_names)):
        if (fields_names[i] == "value"):
            variables =  (variable_descriptions[i].split("(")[1])[:-1].split(",")
            variables[len(variables)-1] = variables[len(variables)-1][5:]
            for variable in variables:
                variable = variable.split(" for ")
                units = variable[0]
                var_name = variable[1]
                variable_list.append(var_name)
        elif(fields_names[i] == "variable"):
            pass
    
    variable_list.remove("value")
    variable_list.remove("variable")

    add_variables(variable_list, dataset, column_names)
    tree = ET.ElementTree(dataset)
    ET.indent(tree)

    return tree

def get_bbox(df):
    return [
        float(df['latitude'].max()),
        float(df['latitude'].min()),
        float(df['longitude'].max()),
        float(df['longitude'].min()),
    ]

def get_vertical(df):
    return [
        float(df['depth'].max()),
        float(df['depth'].min()),
    ]

def get_instruments(df):
    return df['sensor'].unique()

def add_variables(variable_list, dataset, merged_columns):
    if os.path.exists(variable_config_file):
        with open(variable_config_file) as f:
            variable_config = yaml.load(f, Loader=yaml.FullLoader)
    else:
        variable_config = {}
    
    new_variable_found = False
    # variable = []
    # archived_vars = []
    # same_variables = True
    # for var in variable_config:
    #     archived_vars.append(var)
    # for variable in variable_list:
    #     if variable not in archived_vars:
    #         same_variables = False
    #         break
    # if same_variables == True:
    #     variable_list = archived_vars
    # print(variable_list)
    # for variable in variable_list:
    #     if variable not in variable_config:
    #         variable_config[variable] = {
    #             "destinationName": '',
    #             "dataType": '',
    #             "attributes": {
    #                 "ioos_category": {
    #                     "value" : '',
    #                 },
    #                 "long_name" : {
    #                     "value" : '',
    #                 },
    #             },
    #         }
    #         new_variable_found = True
    #     else:
    #         dataVariable = ET.SubElement(dataset, "dataVariable")
    #         ET.SubElement(dataVariable, "sourceName").text = variable
    #         ET.SubElement(dataVariable, "destinationName").text = variable_config[variable]["destinationName"]
    #         ET.SubElement(dataVariable, "dataType").text = variable_config[variable]["dataType"]
    #         addAttributes = ET.SubElement(dataVariable, "addAttributes")
    #         for k, v in variable_config[variable]["attributes"].items():
    #             if "type" in variable_config[variable]["attributes"][k]:
    #                 ET.SubElement(addAttributes, "att", type = variable_config[variable]["attributes"][k]["type"], name = k).text = variable_config[variable]["attributes"][k]["value"]
    #             else:
    #                 ET.SubElement(addAttributes, "att", name = k).text = variable_config[variable]["attributes"][k]["value"]

    for variable in merged_columns:
        if variable not in variable_config:
            variable_config[variable] = {
                "destinationName": '',
                "dataType": '',
                "attributes": {
                    "ioos_category": {
                        "value" : '',
                    },
                    "long_name" : {
                        "value" : '',
                    },
                },
            }
            new_variable_found = True

        
    if new_variable_found:
        with open(variable_config_file, 'w') as f:
            yaml.dump(variable_config, f)
        raise Exception("New variables found, please update the following config file with additional information: %s" % variable_config_file)

    merged_columns.sort()
    
    for variable in merged_columns:
        dataVariable = ET.SubElement(dataset, "dataVariable")
        ET.SubElement(dataVariable, "sourceName").text = variable
        ET.SubElement(dataVariable, "destinationName").text = variable_config[variable]["destinationName"]
        ET.SubElement(dataVariable, "dataType").text = variable_config[variable]["dataType"]
        addAttributes = ET.SubElement(dataVariable, "addAttributes")
        for k, v in variable_config[variable]["attributes"].items():
            if "type" in variable_config[variable]["attributes"][k]:
                ET.SubElement(addAttributes, "att", type = variable_config[variable]["attributes"][k]["type"], name = k).text = variable_config[variable]["attributes"][k]["value"]
            else:
                ET.SubElement(addAttributes, "att", name = k).text = variable_config[variable]["attributes"][k]["value"]


def main(dataset_id, data_file, outputFolder=None):
    df = pd.read_csv(data_file, parse_dates=['timestamp'])
    metadata = generate_from_metadata(dataset_id, df, data_file)

    base_filename = os.path.splitext(os.path.basename(data_file))[0]

    xmlName = os.path.join(
        outputFolder,
        "%s.xml" % base_filename
    )

    metadata.write(xmlName)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # args = sys.argv[1:]
    parser.add_argument("SetID", type=str,
                    help="Dataset ID")
    parser.add_argument("dataFile", type=str, help="Corresponding data file")
    parser.add_argument("-o", help="custom name of output directory")
    args = parser.parse_args()
    outputFolder = None
    if args.o:
        outputFolder = args.o
        print("Files will be outputted in %s" %args.o)
    dataset_id = args.SetID
    main(dataset_id, args.dataFile, outputFolder)