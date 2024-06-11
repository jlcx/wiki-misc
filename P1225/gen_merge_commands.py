#!/usr/bin/env python3
"""an attempt to reconstruct my approach to merging P1225 duplicates, from IPython history"""

import json

with open('P1225_items_unpretty.json') as infile:
    items = {}
    for l in infile:
        obj = json.loads(l)
        items[obj['id']] = obj

P1225_ids = {}

for item in items:
    usnai = items[item]['claims']['P1225'][0]['mainsnak']['datavalue']['value']
    if usnai in P1225_ids:
        P1225_ids[usnai].append(item)
    else:
        P1225_ids[usnai] = [item]

for i in P1225_ids:
    if len(P1225_ids[i]) > 1:
        print(i, P1225_ids[i])

print()
print('The following items should be merged (merge commands in QuickStatements V1 format):')

for i in P1225_ids:
    d = P1225_ids[i]
    if len(d) == 2:
        print('MERGE\t' + d[1] + '\t' + d[0])
