#!/usr/bin/env python3
"""Generate QuickStatements V2 CSV batches that overwrite the bad scraped
English descriptions in items.json with proper "<nationality> <occupation>
(years)" descriptions, following Wikidata description style.

Two CSVs are written:
  * en_descs.csv           — items confidently treated as Danish.
  * en_descs.tentative.csv — items where place evidence (description text
                              or P19/P20/P119 claims) suggests a non-Danish
                              nationality. Each row uses the inferred
                              foreign nationality, but should be reviewed
                              before submitting.
"""

import argparse
import csv
import gzip
import json
import re
import sys
import time
import urllib.request
import zlib

WD_API = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json'
USER_AGENT = 'En2Scandi/0.1 (https://www.wikidata.org/wiki/User:Jamie7687)'
QUERY_LIMIT = 50

# Denmark, Kingdom of Denmark
DANISH_COUNTRIES = {'Q35', 'Q756617'}

# Danish-language country names whose presence in a place string indicates
# the place is outside Denmark. The value is the English nationality used
# when suggesting a description in the review file.
FOREIGN_NATIONALITY = {
    'Norge': 'Norwegian',
    'Sverige': 'Swedish',
    'Finland': 'Finnish',
    'Island': 'Icelandic',
    'Færøerne': 'Faroese',
    'Grønland': 'Greenlandic',
    'Tyskland': 'German',
    'Polen': 'Polish',
    'Rusland': 'Russian',
    'Estland': 'Estonian',
    'Letland': 'Latvian',
    'Litauen': 'Lithuanian',
    'England': 'English',
    'Skotland': 'Scottish',
    'Wales': 'Welsh',
    'Irland': 'Irish',
    'USA': 'American',
    'Canada': 'Canadian',
    'Frankrig': 'French',
    'Italien': 'Italian',
    'Spanien': 'Spanish',
    'Portugal': 'Portuguese',
    'Holland': 'Dutch',
    'Nederlandene': 'Dutch',
    'Belgien': 'Belgian',
    'Schweiz': 'Swiss',
    'Østrig': 'Austrian',
    'Grækenland': 'Greek',
    'Tjekkiet': 'Czech',
    'Ungarn': 'Hungarian',
    'Rumænien': 'Romanian',
}

FOREIGN_RE = re.compile(r'\b(' + '|'.join(map(re.escape, FOREIGN_NATIONALITY)) + r')\b')

DESC_BIRTH_PLACE_RE = re.compile(
    r'Født:\s*[\d\-]+(?:\s+i\s+([^()]+?))?\s+(?:Død:|Gravsted:|\()'
)
DESC_GRAVE_RE = re.compile(r'Gravsted:\s*(.+)$')


def fetch_entities(qids, props='claims|labels'):
    """Batched wbgetentities call. Returns dict {qid: entity}."""
    out = {}
    for i in range(0, len(qids), QUERY_LIMIT):
        chunk = qids[i:i + QUERY_LIMIT]
        url = f'{WD_API}&props={props}&ids={"|".join(chunk)}'
        req = urllib.request.Request(url)
        req.add_header('User-Agent', USER_AGENT)
        req.add_header('Accept-Encoding', 'gzip,deflate')
        time.sleep(1)
        with urllib.request.urlopen(req) as r:
            enc = r.info().get('Content-Encoding')
            raw = r.read()
        if enc == 'gzip':
            raw = gzip.decompress(raw)
        elif enc == 'deflate':
            raw = zlib.decompress(raw, -zlib.MAX_WBITS)
        result = json.loads(raw.decode('utf-8'))
        if result.get('success') != 1:
            err = result.get('error', {}).get('info', '?')
            raise RuntimeError(f'wbgetentities failed: {err}')
        out.update(result['entities'])
    return out


def claim_item_ids(ent, prop):
    out = []
    for c in ent.get('claims', {}).get(prop, []):
        ms = c.get('mainsnak', {})
        if ms.get('snaktype') != 'value':
            continue
        dv = ms.get('datavalue', {}).get('value', {})
        if isinstance(dv, dict) and 'id' in dv:
            out.append(dv['id'])
    return out


def claim_year(ent, prop):
    for c in ent.get('claims', {}).get(prop, []):
        ms = c.get('mainsnak', {})
        if ms.get('snaktype') != 'value':
            continue
        t = ms.get('datavalue', {}).get('value', {}).get('time', '')
        m = re.match(r'^[+-](\d+)-', t)
        if m:
            return int(m.group(1))
    return None


def description_places(desc):
    places = []
    m = DESC_BIRTH_PLACE_RE.search(desc)
    if m and m.group(1):
        places.append(m.group(1).strip())
    m = DESC_GRAVE_RE.search(desc)
    if m and m.group(1).strip() != 'Manglende oplysning':
        places.append(m.group(1).strip())
    return places


def foreign_tokens_in(places):
    seen = []
    for s in places:
        for m in FOREIGN_RE.finditer(s):
            tok = m.group(1)
            if tok not in seen:
                seen.append(tok)
    return seen


def join_occupations(labels):
    if not labels:
        return None
    if len(labels) == 1:
        return labels[0]
    # English list style without Oxford comma: "a, b and c"
    return ', '.join(labels[:-1]) + ' and ' + labels[-1]


def format_description(nationality, occupation, birth_year, death_year):
    head = f'{nationality} {occupation or "person"}'
    if birth_year and death_year:
        return f'{head} ({birth_year}–{death_year})'
    if birth_year:
        return f'{head} (born {birth_year})'
    if death_year:
        return f'{head} (died {death_year})'
    return head


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('items_json', nargs='?', default='items.json')
    ap.add_argument('-o', '--out', default='en_descs.csv',
                    help='QS V2 CSV for confidently-Danish items (default: en_descs.csv)')
    ap.add_argument('-t', '--tentative', default='en_descs.tentative.csv',
                    help='QS V2 CSV for items with non-Danish evidence — review before use '
                         '(default: en_descs.tentative.csv)')
    args = ap.parse_args()

    with open(args.items_json) as f:
        items = json.load(f)

    # Collect unique place + occupation QIDs to look up in one pass
    place_qids = set()
    occ_qids = set()
    for ent in items.values():
        for prop in ('P19', 'P20', 'P119'):
            place_qids.update(claim_item_ids(ent, prop))
        occ_qids.update(claim_item_ids(ent, 'P106'))

    looked_up = fetch_entities(sorted(place_qids | occ_qids)) if (place_qids or occ_qids) else {}

    place_country = {q: claim_item_ids(looked_up.get(q, {}), 'P17') for q in place_qids}
    occ_label = {
        q: looked_up.get(q, {}).get('labels', {}).get('en', {}).get('value')
        for q in occ_qids
    }

    n_emit = n_tent = 0
    with open(args.out, 'w', newline='', encoding='utf-8') as out_f, \
         open(args.tentative, 'w', newline='', encoding='utf-8') as tent_f:

        out_w = csv.writer(out_f)
        tent_w = csv.writer(tent_f)
        out_w.writerow(['qid', 'Den'])
        tent_w.writerow(['qid', 'Den'])

        for qid, ent in items.items():
            en_desc = ent['descriptions']['en']['value']

            ftoks = foreign_tokens_in(description_places(en_desc))
            non_dk_country_qids = set()
            for prop in ('P19', 'P20', 'P119'):
                for pq in claim_item_ids(ent, prop):
                    cs = place_country.get(pq, [])
                    if cs and not (set(cs) & DANISH_COUNTRIES):
                        non_dk_country_qids.update(cs)

            b = claim_year(ent, 'P569')
            d = claim_year(ent, 'P570')
            occ_str = join_occupations([
                occ_label[q] for q in claim_item_ids(ent, 'P106') if occ_label.get(q)
            ])

            if ftoks or non_dk_country_qids:
                # Pick a nationality for the tentative row. If exactly one
                # foreign country surfaced, use it; otherwise fall back to a
                # placeholder so the user knows to fix it before submitting.
                if len(ftoks) == 1:
                    nationality = FOREIGN_NATIONALITY[ftoks[0]]
                else:
                    nationality = '???'
                tent_w.writerow([qid, format_description(nationality, occ_str, b, d)])
                n_tent += 1
            else:
                out_w.writerow([qid, format_description('Danish', occ_str, b, d)])
                n_emit += 1

    print(f'Wrote {n_emit} confident statements to {args.out}', file=sys.stderr)
    print(f'Wrote {n_tent} tentative statements to {args.tentative}', file=sys.stderr)


if __name__ == '__main__':
    main()
