#!/usr/bin/env python3
"""fix some specific descriptions in Wikidata"""

import time
import json
import urllib.request

wd_url = 'https://wikidata.org/wiki/'
skipchars = len(wd_url)
# the API URL to get detailed info on multiple items from Wikidata
url_base = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&ids='
# the documented limit on how many items can be queried at once
query_limit = 50

def get_entities(query_ids):
    ids_joined = '|'.join(query_ids)
    req = urllib.request.Request(url_base + ids_joined)
    req.add_header('User-Agent', 'ObitScript/0.1 (https://www.wikidata.org/wiki/User:Jamie7687)')
    result = urllib.request.urlopen(req)
    result_json = json.loads(result.read())
    if result_json['success'] == 1:
        return result_json['entities']
    else:
        raise Exception('wbgetentities call failed')

with open('bad_bits.txt') as badfile:
    ids = []
    entities_full = {}

    for line in badfile:
        qid = line.strip().split(' ', 1)[0]
        ids.append(qid)
        
    entity_groups = [ids[i:i+50] for i in range(0, len(ids), 50)]
    
    for eg in entity_groups:
            entities_full.update(get_entities(eg))
            time.sleep(1)
    with open('obit_entities.json', 'w') as efile:
        json.dump(entities_full, efile, indent=4)

    # I executed the following in IPython        
    #with open('obit_entities.json') as infile:
    #    entities = json.load(infile)
 
    errors = []

    for e in entities_full:
        obj = entities_full[e]
        qid = obj['id']
        label = obj['labels']['en']['value']
        desc = obj['descriptions']['en']['value']
        try:
            oindex = desc.index('Obituary')
            newdesc = desc[len(label)+1:oindex+8]
            if ',' not in newdesc:
                print(qid + ',' + newdesc)
        except:
            errors.append((qid, desc))
            
    print(errors)
        
