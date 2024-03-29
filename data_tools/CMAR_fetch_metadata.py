import requests
import argparse
import os
import yaml
from datetime import datetime
import uuid
import hashlib
import pandas as pd

sensor_config_file = os.path.join(os.path.dirname(__file__), '..', 'sensors.yaml')

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
    description += 'Contains information licensed under the Open Government Licence – Nova Scotia. If you have accessed any of the Coastal Monitoring Program data, CMAR would appreciate your feedback through this quick '
    description += '[questionaire](https://docs.google.com/forms/d/e/1FAIpQLSe3TD6umrsVVKnQL13VVMJIpckCi2ctONJsgN7_g-4c-tKTuw/viewform).'
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
        raise Exception('Licence is not Nova Scotia Open Government Licence. \t Found Licence:%s' % license)

    lang = 'en'
    if (language == 'eng'):
        lang = 'en'
    else:
        lang = 'fr'

    #Creates an eovEN and eovFR with the preset eovs present in the 'tags' of the metadata. 
    checkEOV = ['salinity','temperature','dissolved oxygen']
    eovENList = ['Sub Surface Salinity','Sub Surface Temperature', 'Oxygen']
    eovFRList = ['Salinité sous la surface', 'Température sous la surface', 'Oxygène']
    eovEN = []
    eovFR = []
    for keyword in keywords:
        count = 0
        for eov in checkEOV:
            if eov in keyword:
                eovEN.append(eovENList[count])
                eovFR.append(eovFRList[count])
            count += 1

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
    dist.append({'url': 'https://cioosatlantic.ca/erddap/tabledap/'+dataset_id+'.html', 'name':'ERDDAP Data Access'})

    dict_file = {
        'metadata' : {
            'naming_authority': 'ca.cioos',
            'identifier': str(uuid.uuid3(uuid.UUID(bytes=hashlib.md5(b"CMAR").digest()), dataset_id)), 
            'language': lang,
            'maintenance_note':'Generated from https://cioos-siooc.github.io/metadata-entry-form',
            'use_constraints' : {
                'limitations' : {
                    'en' : "Licence",
                    'fr' : "Licence",
                },
                'licence' : {
                    'title' : 'Open Government Licence - Nova Scotia',
                    'code' : 'OGL-NS-2.0',
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
            # 'temporal_begin': temporal_begin.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
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

def get_temporal_begin(df):
    date = df['timestamp'].min()
    date = date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    return date

def get_spatial(df):
    return {
        'bbox' : get_bbox(df), 'vertical' : get_vertical(df)
    }

def get_instruments(df, platform):
    return df[df['waterbody_station'] == platform]['sensor'].unique()

def guess_manufacturer(instrument, instrument_config):
    partial_name = instrument.split('-')
    if len(partial_name) >= 2:
        for i in instrument_config:
            if i.startswith(partial_name[0]) and instrument_config[i]['manufacturer'] != '':
                return instrument_config[i]['manufacturer']
    return None

def get_platforms(df):
    platforms = []
    waterbody_platforms = df['waterbody_station'].unique()
    for water_platform in waterbody_platforms:
        platforms.append(water_platform.split('-',1)[1])

    platform_metadata = []
    index = 0
    for platform in platforms:
        instrument_list = get_instruments(df, waterbody_platforms[index])
        index += 1
        if os.path.exists(sensor_config_file):
            with open(sensor_config_file) as f:
                instrument_config = yaml.load(f, Loader=yaml.FullLoader)
        else:
            instrument_config = {}
        
        new_instrument_found = False
        new_instruments_no_match_found = False
        instruments = []
        for instrument in instrument_list:
            if instrument not in instrument_config:
                manufacturer = guess_manufacturer(instrument, instrument_config)
                if manufacturer is None:
                    manufacturer = ''
                    new_instruments_no_match_found = True

                instrument_config[instrument] = {
                    "id": instrument,
                    "manufacturer": manufacturer,
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
            if new_instruments_no_match_found:
                raise Exception("New sensors found, please update the following config file with additional information: %s" % sensor_config_file)
            else:
                print("New sensors were found but it was possible to infer the manufacturer.")

        platform_metadata.append({
            "id": platform,
            "description": {"en": platform},
            "instruments": instruments
        })
    
    if len(platform_metadata) == 1:
        return platform_metadata[0]
    else:
        return platform_metadata

def generate_metadata_from_data(metadata, data_file):
    df = pd.read_csv(data_file, parse_dates=['timestamp'])
    metadata['identification']['temporal_begin'] = get_temporal_begin(df)
    metadata['spatial'] = get_spatial(df)
    platform = get_platforms(df)
    metadata["platform"] = platform
    return metadata

def main(dataset_id, data_file, output_directory):
    metadata = generate_from_metadata(dataset_id)
    updated_metadata = generate_metadata_from_data(metadata, data_file)
    
    base_filename = os.path.splitext(os.path.basename(data_file))[0]

    yamlName = os.path.join(
        output_directory,
        "%s.yaml" % base_filename
    )
        
    with open(yamlName, 'w', encoding='utf8') as f:
        data = yaml.dump(updated_metadata, f, allow_unicode=True, sort_keys=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("dataset_id", type=str,
                    help="Dataset ID")
    parser.add_argument("data_file", type=str, help="Corresponding data file")
    parser.add_argument("output_directory", help="custom name of output directory")

    args = parser.parse_args()

    dataset_id = args.dataset_id
    data_file = args.data_file
    output_directory = args.output_directory

    main(dataset_id, data_file, output_directory)
