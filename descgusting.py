#!/usr/bin/env python3
"""print bad Wikidata descriptions and the item they belong to"""

import sys
import string
import json

# zcat latest-all.json.gz | ./descgusting.py
with sys.stdin as infile:
    infile.readline()
    for line in infile:
        obj = json.loads(line.rstrip(',\n'))
        qid = obj['id']
        if 'en' in obj['descriptions']:
            desc = obj['descriptions']['en']['value']
            
            startswith_label = False
            if 'en' in obj['labels'] and desc.startswith(obj['labels']['en']['value']):
                startswith_label = True
            too_long = len(desc) > 140
            capped = desc[0].isupper()
            punct = desc[-1] in string.punctuation
            rr = '®' in desc
            tm = '™' in desc
            bad_starts = ('a ', 'an ', 'the ', 'A ', 'An ', 'The ', 'It ', 'is ', 'are ', 'was ', 'were ')
            
            if startswith_label and too_long and capped and punct:
                print(qid, ' ' * (16 - len(qid)), desc)
        
            
