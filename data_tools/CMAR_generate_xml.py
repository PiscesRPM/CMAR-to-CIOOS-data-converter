import requests
import argparse
import os
import yaml
from datetime import datetime
import uuid
import pandas as pd
from xml.etree.ElementTree import ElementTree, tostring
import xml.etree.cElementTree as ET

variable_config_file = os.path.join(os.path.dirname(__file__), '..', 'variables.yaml')

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
    #ET.SubElement(dataset, "fileDir").text = "/datasets/cmar/"
    #ET.SubElement(dataset, "fileNameRegex").text = ".*merged\.csv"
    ET.SubElement(dataset, "fileDir").text = "/datasets/cmar/nsodp-sensor-strings/" + str(dataset_id)
    ET.SubElement(dataset, "fileNameRegex").text = ".*[0-9][0-9]\.csv"
    ET.SubElement(dataset, "recursive").text = "true"
    ET.SubElement(dataset, "pathRegex").text = ".*"
    ET.SubElement(dataset, "metadataFrom").text = "last"
    ET.SubElement(dataset, "standardizeWhat").text = "0"
    ET.SubElement(dataset, "charset").text = "ISO-8859-1"
    ET.SubElement(dataset, "columnSeparator").text = ","
    ET.SubElement(dataset, "columnNamesRow").text = "1"
    ET.SubElement(dataset, "firstDataRow").text = "2"
    ET.SubElement(dataset, "sortedColumnSourceName").text = "timestamp_utc"
    ET.SubElement(dataset, "sortFilesBySourceNames").text = "timestamp_utc"
    ET.SubElement(dataset, "fileTableInMemory").text = "false"
    ET.SubElement(dataset, "accessibleViaFiles").text = "true"

    addAttributes = ET.SubElement(dataset, "addAttributes")
    ET.SubElement(addAttributes, "att", name = "cdm_data_type").text = "TimeSeries"
    ET.SubElement(addAttributes, "att", name = "cdm_timeseries_variables").text = "waterbody,station,sensor_type,sensor_serial_number"
    ET.SubElement(addAttributes, "att", name = "Conventions").text = "COARDS, CF-1.6, ACDD-1.3"
    ET.SubElement(addAttributes, "att", name = "creator_name").text = creator_name
    ET.SubElement(addAttributes, "att", name = "creator_type").text = 'institution'
    ET.SubElement(addAttributes, "att", name = "infoUrl").text = '???'
    ET.SubElement(addAttributes, "att", name = "institution").text = creator_name
    ET.SubElement(addAttributes, "att", name = "license").text = license
    ET.SubElement(addAttributes, "att", name = "sourceUrl").text = "(local files)"
    ET.SubElement(addAttributes, "att", name = "standard_name_vocabulary").text = "CF Standard Name Table v55" 
    #ET.SubElement(addAttributes, "att", name = "subsetVariables").text = "waterbody_station, lease_number, sensor_type, sensor_serial_number, qc_flag_dissolved_oxygen_percent_saturation, qc_flag_temperature, qc_flag_salinity, depth_crosscheck_flag, qc_flag_sensor_depth_measured" #ASK
    #ET.SubElement(addAttributes, "att", name = "subsetVariables").text = "waterbody, station, sensor_type, sensor_serial_number" #ASK
    
    subset_variables = "waterbody, station, sensor_type, sensor_serial_number"
    if("lease" in column_names):
        subset_variables+= ",lease"
    if("string_configuration" in column_names):
        subset_variables+= ",string_configuration"
    if("qc_flag_dissolved_oxygen_percent_saturation" in column_names):
        subset_variables+= ",qc_flag_dissolved_oxygen"
    if("qc_flag_dissolved_oxygen_uncorrected_mg_per_l" in column_names):
        subset_variables+= ",qc_flag_dissolved_oxygen_uncorrected"
    if("qc_flag_salinity_psu" in column_names):
        subset_variables+= ",qc_flag_salinity"
    if("qc_flag_sensor_depth_measured_m" in column_names):
        subset_variables+= ",qc_flag_sensor_depth_measured"
    if("qc_flag_temperature_degree_c" in column_names):
        subset_variables+= ",qc_flag_temperature"
    if("depth_crosscheck_flag" in column_names):
        subset_variables+= ",depth_crosscheck_flag"
    ET.SubElement(addAttributes, "att", name = "subsetVariables").text = subset_variables
    
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
    # TODO: replace this old way of doing things:
    # for i in range(len(fields_names)):
    #     if (fields_names[i] == "value"):
    #         variables =  (variable_descriptions[i].split("(")[1])[:-1].split(",")
    #         variables[len(variables)-1] = variables[len(variables)-1][5:]
    #         for variable in variables:
    #             variable = variable.split(" for ")
    #             units = variable[0]
    #             var_name = variable[1]
    #             variable_list.append(var_name)
    #     elif(fields_names[i] == "variable"):
    #         pass
    
    # variable_list.remove("value")
    # variable_list.remove("variable")

    # TODO: write code that opens the final output, parses the column names and creates a structure: variable_list = [(var_name, units),(var_name, units),(var_name, units)]
    
    variable_list = extract_variables(df)

    add_variables(variable_list, dataset, column_names)
    
    tree = ET.ElementTree(dataset)
    ET.indent(tree)
    return tree

def extract_variables(df):
    ignored_columns = ['waterbody', 'station', 'lease', 'latitude', 'longitude',
       'deployment_start_date', 'deployment_end_date', 'timestamp_utc', 'sensor_type', 'sensor_serial_number',
       'sensor_depth_at_low_tide_m', 'mooring']

    variable_list = {}
    
    for col in df.columns:
        if col in ignored_columns:
            pass
        else:
            split_col = col.split("_")
            if len(split_col) > 1:
                variable_list[col] = split_col
    
    return variable_list

def get_bbox(df):
    return [
        float(df['latitude'].max()),
        float(df['latitude'].min()),
        float(df['longitude'].max()),
        float(df['longitude'].min()),
    ]

def get_vertical(df):
    return [
        float(df['sensor_depth_at_low_tide_m'].max()),
        float(df['sensor_depth_at_low_tide_m'].min()),
    ]

def get_instruments(df):
    return df.apply(lambda x:'%s-%s' % (x['sensor_type'],x['sensor_serial_number']),axis=1).unique()

def add_variables(variable_list, dataset, merged_columns):
    # TODO: update this function so that it understand the new variable_list format([(variable_name, units),(variable_name, units)])
    # Make sure the function uses the units from the variable_list
    if os.path.exists(variable_config_file):
        with open(variable_config_file) as f:
            variable_config = yaml.load(f, Loader=yaml.FullLoader)
    else:
        variable_config = {}
    
    new_variable_found = False

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
            
            if variable in variable_list:
                variable_config[variable]["attributes"]["units"] = {
                    "value": variable_list[variable][1]
                }
                variable_config[variable]["attributes"]["ioos_category"]["value"] = variable_list[variable][0] 
                variable_config[variable]["attributes"]["long_name"]["value"] = variable_list[variable][0] 
                variable_config[variable]["dataType"] = "float"
                variable_config[variable]["destinationName"] = variable
            new_variable_found = True

        
    if new_variable_found:
        with open(variable_config_file, 'w') as f:
            yaml.dump(variable_config, f)
        raise Exception("New variables found, please update the following config file with additional information: %s" % variable_config_file)

    # Get the columns in a nice order
    sorted_columns = []
    if("waterbody" in merged_columns):
        sorted_columns.append("waterbody")
    if("station" in merged_columns):
        sorted_columns.append("station")
    if("lease" in merged_columns):
        sorted_columns.append("lease")
    if("latitude" in merged_columns):
        sorted_columns.append("latitude")
    if("longitude" in merged_columns):
        sorted_columns.append("longitude")
    if("deployment_start_date" in merged_columns):
        sorted_columns.append("deployment_start_date")
    if("deployment_end_date" in merged_columns):
        sorted_columns.append("deployment_end_date")
    if("timestamp_utc" in merged_columns):
        sorted_columns.append("timestamp_utc")
    if("sensor_depth_at_low_tide_m" in merged_columns):
        sorted_columns.append("sensor_depth_at_low_tide_m")
    if("sensor_type" in merged_columns):
        sorted_columns.append("sensor_type")
    if("sensor_serial_number" in merged_columns):
        sorted_columns.append("sensor_serial_number")
    
    for col in merged_columns:
        if col not in sorted_columns:
            sorted_columns.append(col)
    merged_columns = sorted_columns
    
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
    df = pd.read_csv(data_file, parse_dates=['timestamp_utc'])
    metadata = generate_from_metadata(dataset_id, df, data_file)

    base_filename = os.path.splitext(os.path.basename(data_file))[0]

    xmlName = os.path.join(
        outputFolder,
        "%s.xml" % base_filename
    )

    metadata.write(xmlName)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
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