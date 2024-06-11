#!/usr/bin/env python3

import time
import json
import urllib.request

# the API URL to get detailed info on multiple items from Wikidata
url_base = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&ids='
# the documented limit on how many items can be queried at once
query_limit = 50

file_path = 'wikidata-Garyh.stern-ALL-ids.csv'

def get_entities(query_ids):
    ids_joined = '|'.join(query_ids)
    req = urllib.request.Request(url_base + ids_joined)
    req.add_header('User-Agent', 'ItemsDownloaderP1225/0.1 (https://www.wikidata.org/wiki/User:Jamie7687)')
    # req.add_header('Accept-Encoding', 'gzip')
    time.sleep(1)
    result = urllib.request.urlopen(req)
    result_json = json.loads(result.read())
    if result_json['success'] == 1:
        return result_json['entities']
    else:
        raise Exception('wbgetentities call failed')
    
if __name__ == '__main__':
    with open(file_path, 'r') as f:
        ids = f.read().splitlines()

    # split the list of IDs into chunks of query_limit size to avoid hitting the limit
    for i in range(0, len(ids), query_limit):
        query_ids = ids[i:i+query_limit]
        entities = get_entities(query_ids)
        for entity in entities:
            print(json.dumps(entities[entity]))
    # redirect output to a file to save the results (e.g. python3 download_P1225_items.py > P1225_items.json)