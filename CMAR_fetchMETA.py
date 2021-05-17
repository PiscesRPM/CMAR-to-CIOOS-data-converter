import requests
import argparse
import os
import yaml
from datetime import datetime
import uuid
import pandas as pd

sensor_config_file = 'sensors.yaml'

url = 'https://data.novascotia.ca/api/views/metadata/v1/x9dy-aai9'
def get_metadata(dataset_id):
    url = 'https://data.novascotia.ca/api/views/metadata/v1/'
    url = url + dataset_id
    return requests.get(url).json()

def generate_from_metadata(dataset_id):
    metadata = get_metadata(dataset_id)

    title = metadata['name'] #title
    createDate = metadata['createdAt'] #creattion + publication
    updateDate = metadata['dataUpdatedAt'] #revision
    description = metadata['customFields']['Detailed Metadata']['Usage Considerations'] #abstract:
    keywords = metadata['tags'] #keywords
    language = metadata['customFields']['Detailed Metadata']['Language']
    orgName = "Centre for Marine Applied Research (CMAR)"
    orgPosition = "Author/Owner"
    orgEmail = "info(at)cmar.ca"
    distribution = metadata['customFields']['Detailed Metadata']['Related Documents']
    status = 'published'
    progress_code = 'onGoing'
    roles = ['owner','custodian','author','distributor']
    license = metadata['license']

    if (license != 'Nova Scotia Open Government Licence'):
        raise Exception('License is not Nova Scotia Open Government License. \t Found License:%s' % license)

    lang = 'en'
    if (language == 'eng'):
        lang = 'en'
    else:
        lang = 'fr'

    eovEN = ['subSurfaceSalinity','subSurfaceTemperature', 'dissolvedOrganicCarbon']
    eovFR = ['Salinité sous la surface', 'Température sous la surface', 'carbone inorganique dissous']
    temporal_begin = datetime.strptime(
        createDate,
        '%Y-%m-%dT%H:%M:%S+%f'
    )

    distributions = distribution.split(" and ")
    dist = []
    for distri in distributions:
        distName = distri.split("(",1)[0]
        distURL = distri.split("(",1)[1]
        distName = distName[:-1]
        distURL = distURL.split(")",1)[0]
        distAdd  = {'url': distURL, 'name':distName}
        dist.append(distAdd)
    #dist = [{'url': distURL, 'name':distName},{'url': distURL, 'name':distName}]


    dict_file = {
        'metadata' : {
            'naming_authority': 'ca.coos',
            'identifier': str(uuid.uuid4()), 
            'language': lang,
            'maintenance_note':'Generated from https://cioos-siooc.github.io/metadata-entry-form',
            'use_constraints' : {
                'limitations' : {
                    'en' : "License",
                    'fr' : "License",
                },
                'license' : {
                    'title' : 'Open Government License - Nova Scotia',
                    'code' : 'government-open-license-nova-scotia',
                    'url' : 'https://novascotia.ca/opendata/licence.asp',
                }
            },
            'dates':{
                'revision':datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                'publication':datetime.utcnow().strftime('%Y-%m-%d')
            }
        },
        #add spatial
        'spatial': {
            'bbox': [],
            'vertical': []
        },
        'identification' : {
            'title' : {
                'en': title,
                'fr':''
            },
            'abstract':{
                'en':description,
                'fr':''
            }, 
            'dates':{
                'creation':createDate.split('T',1)[0],
                'publication':createDate.split('T',1)[0],
                'revision':updateDate.split('T',1)[0]
            },
            'keywords':{
                'default':{
                    'en':keywords
                },
                'eov':{
                    'en':eovEN,
                    'fr':eovFR
                }
            },
            'temporal_begin': temporal_begin.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
            'status': status,
            'progress_code': progress_code
        },
        'contact':[
            {'roles': roles, 
            'organization': {
                'name': orgName
            }, 
            'individual': {
                'position':orgPosition,
                'email':orgEmail
            }
        }],
        'distribution' : dist, 
        # add platform 'platform':{'id':platformID,'description':{'en':platformID},'instruments':}
    }
    return dict_file

def get_bbox(df):
    return [
        float(df['longitude'].min()),
        float(df['latitude'].min()),
        float(df['longitude'].max()),
        float(df['latitude'].max())
    ]

def get_vertical(df):
    return [
        float(df['depth'].min()),
        float(df['depth'].max())
    ]

def get_spatial(df):
    return {
        'bbox' : get_bbox(df), 'vertical' : get_vertical(df)
    }

def get_instruments(df):
    return df['sensor'].unique()

def get_platform(df):
    platforms = df['station'].unique()
    if len(platforms) > 1:
        raise Exception("This data file includes multiple stations")
    platform = platforms[0]

    instrument_list = get_instruments(df)
    if os.path.exists(sensor_config_file):
        with open(sensor_config_file) as f:
            instrument_config = yaml.load(f, Loader=yaml.FullLoader)
    else:
        instrument_config = {}
    
    new_instrument_found = False
    instruments = []
    for instrument in instrument_list:
        if instrument not in instrument_config:
            instrument_config[instrument] = {
                "id": instrument,
                "manufacturer": '',
                "type": {
                    "en": "Sensor",
                    "fr": ""
                },
                "version": ""
            }
            new_instrument_found = True
        else:
            instruments.append(instrument_config[instrument]) 
    
    if new_instrument_found:
        with open(sensor_config_file, 'w') as f:
            yaml.dump(instrument_config, f)
        raise Exception("New sensors found, please update the following config file with additional information: %s" % sensor_config_file)

    return {
        "id": platform,
        "description": {"en": platform},
        "instruments": instruments
    }

def generate_metadata_from_data(metadata, data_file):
    df = pd.read_csv(data_file, parse_dates=['timestamp'])

    metadata['spatial'] = get_spatial(df)

    platform = get_platform(df)
    metadata["platform"] = platform
    return metadata

def main(dataset_id, data_file, outputFolder=None):
    metadata = generate_from_metadata(dataset_id)
    print(metadata['spatial'])
    updated_metadata = generate_metadata_from_data(metadata, data_file)

    print(updated_metadata['spatial'])

    yamlName = "%s.yaml" % data_file.split(".")[0]
    if outputFolder != None:
        outputFolder = os.path.dirname(__file__) + '/' + outputFolder
        path = os.path.join(os.path.dirname(__file__), outputFolder)
        if not os.path.exists(path):
            os.mkdir(path)
        # yamlName = os.path.join(outputFolder,yamlName)
        yamlName = outputFolder + "/" + yamlName
        
    with open(yamlName, 'w', encoding='utf8') as f:
        data = yaml.dump(updated_metadata, f, allow_unicode=True, sort_keys=False)

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
# while len(metadata) > 0:
#     all_metadata.extend(metadata)
#     page += 1
#     metadata = get_metadata(page)
#     print(metadata)
