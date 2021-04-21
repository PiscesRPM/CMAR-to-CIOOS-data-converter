import requests
import argparse
import os
import yaml
from datetime import datetime
import uuid

url = 'https://data.novascotia.ca/api/views/metadata/v1/x9dy-aai9'
def get_metadata(page, dataset_id):
    url = 'https://data.novascotia.ca/api/views/metadata/v1/'
    url = url + dataset_id
    return requests.get(url).json()

def main():
    parser = argparse.ArgumentParser()
    # args = sys.argv[1:]
    parser.add_argument("SetID", type=str,
                    help="Dataset ID")
    args = parser.parse_args()
    dataset_id = args.SetID
    
    page = 1
    all_metadata = []
    metadata = get_metadata(page,dataset_id)
    print(metadata)
    print("\n")
    print(metadata['customFields']['Detailed Metadata']['Related Documents'])

    
    title = metadata['name'] #title
    createDate = metadata['createdAt'] #creattion + publication
    updateDate = metadata['dataUpdatedAt'] #revision
    description = metadata['customFields']['Detailed Metadata']['Usage Considerations'] #abstract:
    vertical = [30.0,0.0] #spatial vertical
    keywords = metadata['tags'] #keywords
    language = metadata['customFields']['Detailed Metadata']['Language']
    orgName = "Centre for Marine Applied Research (CMAR)"
    orgPosition = "Author/Owner"
    orgEmail = "info(at)cmar.ca"
    distribution = metadata['customFields']['Detailed Metadata']['Related Documents']
    status = 'published'
    progress_code = 'onGoing'
    roles = ['owner','custodian','author','distributor']

    #dict_file = [{'spatial' : {'bbox': [logitude, latitude, longitude, latidude]}}]
    lang = 'en'
    if (metadata['customFields']['Detailed Metadata']['Language'] == 'eng'):
        lang = 'en'
    else:
        lang = 'fr'



    eovEN = ['subSurfaceSalinity','subSurfaceTemperature', 'dissolvedOrganicCarbon']
    # eovFR = ['Salinité sous la surface','Température sous la surface', 'carbone inorganique dissous']
    # eovFR = [unicode('Salinité sous la surface', "utf-8"),unicode('Température sous la surface', "utf-8"), unicode('Salinité sous la surface', "utf-8")]
    eovFR = ['Salinité sous la surface', 'Température sous la surface', 'carbone inorganique dissous']
    # eovFR = [s.encode(encoding='utf-8') for s in eovFR]
    print(eovFR)
    # temporal_begin = str(createDate) + 'T15:00:00.000Z'
    temporal_begin = createDate

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


    dict_file = [{'metadata' : {'naming_authority': 'ca.coos','identifier': str(uuid.uuid4()), 'language': lang,
                'maintenance_note':'Generated from https://cioos-siooc.github.io/metadata-entry-form',
                'dates':{'revision':datetime.today().strftime('%Y-%m-%d-%H:%M:%S'),'publication':datetime.today().strftime('%Y-%m-%d')}},
                #add spatial
                'identification' : {'title' : {'en': title, 'fr':''},'abstract':{'en':description,'fr':''}, 
                'dates':{'creation':createDate.split('T',1)[0],'publication':createDate.split('T',1)[0],'revision':updateDate.split('T',1)[0]},
                'keywords':{'default':{'en':keywords},'eov':{'en':eovEN,'fr':eovFR}},
                'temporal_begin': temporal_begin, 'status': status,'progress_code': progress_code},
                'contact':{'- roles': roles, 'organization': {'name': orgName}, 'individual': {'position':orgPosition,'email':orgEmail}},
                'distribution' : dist, 
                # add platform 'platform':{'id':platformID,'description':{'en':platformID},'instruments':}
                }]
    print("\n",dict_file)
    yamlName =  "Halifax" + '.yaml' 
    with open(yamlName, 'w', encoding='utf8') as f:
        data = yaml.dump(dict_file, f, allow_unicode=True, sort_keys=False)

    
    



if __name__ == "__main__":
    main()
# while len(metadata) > 0:
#     all_metadata.extend(metadata)
#     page += 1
#     metadata = get_metadata(page)
#     print(metadata)
