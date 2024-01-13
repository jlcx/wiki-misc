#!/usr/bin/env python3
"""look for promotional words in Wikidata descriptions"""

import sys
import string
import json

# zcat latest-all.json.gz | ./not_the_best.py
with sys.stdin as infile:
    infile.readline()
    to_skip = ('Q749290')
    for line in infile:
        obj = json.loads(line.rstrip(',\n'))
        qid = obj['id']
        if qid not in to_skip and 'en' in obj['descriptions']:
            desc = obj['descriptions']['en']['value']
            
            if 'the best ' in desc and 'award' not in desc:
                print(qid, ' ' * (16 - len(qid)), desc)

