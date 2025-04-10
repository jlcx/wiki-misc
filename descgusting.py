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
            extra_space = '  ' in desc
            obit = 'Obituary' in desc
            escape = '&amp;' in desc
            space_comma = ' ,' in desc

            bad_starts = ('a ', 'an ', 'the ', 'A ', 'An ', 'The ', 'It ', 'is ', 'are ', 'was ', 'were ')
            starts_bad = False
            for bs in bad_starts:
                if desc.startswith(bs):
                    starts_bad = True
                    break

            issues = (startswith_label, too_long, capped, punct, rr, tm, starts_bad, extra_space, obit, escape, space_comma)
            score = 0
            threshold = 4

            for i in issues:
                if i:
                    score += 1

            if score >= threshold: #startswith_label and capped and punct:
                print(qid, ' ' * (16 - len(qid)), desc)
