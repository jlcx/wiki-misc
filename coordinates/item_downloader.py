#!/usr/bin/env python3

import time
import re
import json
import urllib.request

# the API URL to get detailed info on multiple items from Wikidata
url_base = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&ids='
# the documented limit on how many items can be queried at once
query_limit = 50

file_path = 'Pasleim_Implausible_coordinate.txt'

def get_entities(query_ids):
    ids_joined = '|'.join(query_ids)
    req = urllib.request.Request(url_base + ids_joined)
    req.add_header('User-Agent', 'ItemsDownloader/0.2 (https://www.wikidata.org/wiki/User:Jamie7687)')
    # Modified line below
    req.add_header('Accept-Encoding', 'gzip,deflate') 
    time.sleep(1)
    result = urllib.request.urlopen(req)
    
    # Handle potential gzip/deflate compressed content
    content_encoding = result.info().get('Content-Encoding')
    if content_encoding == 'gzip':
        import gzip
        data = gzip.decompress(result.read())
    elif content_encoding == 'deflate':
        import zlib
        data = zlib.decompress(result.read(), -zlib.MAX_WBITS) # Negative WBITS for raw deflate
    else:
        data = result.read()
        
    result_json = json.loads(data.decode('utf-8')) # Decode bytes to string before loading JSON
    
    if result_json.get('success') == 1: # Use .get() for safer dictionary access
        return result_json['entities']
    else:
        # Include more error information if available
        error_info = result_json.get('error', {}).get('info', 'Unknown error')
        raise Exception(f'wbgetentities call failed: {error_info}')
    
if __name__ == '__main__':
    try:
        with open(file_path, 'r') as f:
            wikitext = f.read()
            ids = re.findall(r'\[\[(Q[0-9]+)\]\]', wikitext)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        exit(1)
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")
        exit(1)

    all_entities_data = {}
    # split the list of IDs into chunks of query_limit size to avoid hitting the limit
    for i in range(0, len(ids), query_limit):
        query_ids = ids[i:i+query_limit]
        if not query_ids: # Skip if the chunk is empty
            continue
        # print(f"Processing IDs: {', '.join(query_ids)}") # Added for progress tracking
        try:
            entities = get_entities(query_ids)
            for entity_id, entity_data in entities.items(): # Iterate through dictionary items
                # Storing all entities in a dictionary before printing, 
                # or you can print directly as in your original script.
                all_entities_data[entity_id] = entity_data 
                # If you want to print each entity as it's fetched:
                # print(json.dumps(entity_data)) 
        except urllib.error.HTTPError as e:
            print(f"HTTP Error for IDs {', '.join(query_ids)}: {e.code} {e.reason}")
        except urllib.error.URLError as e:
            print(f"URL Error for IDs {', '.join(query_ids)}: {e.reason}")
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON for IDs {', '.join(query_ids)}: {e}")
        except Exception as e:
            print(f"An error occurred while processing IDs {', '.join(query_ids)}: {e}")

    # If you want to print all collected data at the end:
    if all_entities_data:
        print(json.dumps(all_entities_data))
    else:
        print("No data was fetched.")
    # redirect output to a file to save the results (e.g. python3 download_P1225_items.py > P1225_items.json)