#!/usr/bin/env python3
"""output successively longer Wikidata descriptions"""

import sys
import string
import json

# zcat latest-all.json.gz | ./really_long_descriptions.py
with sys.stdin as infile:
    infile.readline()
    to_skip = ('Q31', 'Q8', 'Q75', 'Q178', 'Q1071', 'Q5300', 'Q61905', 'Q15524964', 'Q22669988', 'Q30026965', 'Q47012759', 'Q273948', 'Q420870', 'Q58192', 'Q41377', 'Q148417', 'Q7338', 'Q425024', 'Q180618', 'Q552179', 'Q37011394', 'Q37110257', 'Q46654', 'Q47069', 'Q559003', 'Q613311', 'Q620057', 'Q658145', 'Q671136', 'Q742224', 'Q190200', 'Q742224', 'Q903660', 'Q94427', 'Q915455', 'Q970614', 'Q1083391', 'Q30023157', 'Q1446169', 'Q29644038', 'Q1051110', 'Q1535890', 'Q1279431', 'Q2079841', 'Q1794963', 'Q2264448', 'Q798572', 'Q2093727', 'Q2540295', 'Q42061229', 'Q45736919', 'Q31048074', 'Q47012765', 'Q55095102', 'Q2310773', 'Q3033305', 'Q3123047', 'Q3253731', 'Q3798557')
    longest_id = 'Q'
    longest_desc = ''
    longest_length = 0
    for line in infile:
        obj = json.loads(line.rstrip(',\n'))
        qid = obj['id']
        if qid not in to_skip and 'en' in obj['descriptions']:
            desc = obj['descriptions']['en']['value']
            
            if len(desc) > longest_length:
                print(qid, ' ' * (16 - len(qid)), desc)
                longest_id = qid
                longest_desc = desc
                longest_length = len(desc)

