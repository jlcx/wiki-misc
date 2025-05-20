import json
import time

import urllib.request

from collections import Counter

# --- Geometry Libraries ---
try:
    import geopandas as gpd
    from shapely.geometry import Point # Still need Point from shapely
    from shapely.errors import ShapelyError
except ImportError:
    print("Error: Libraries 'geopandas' and 'shapely' are required.")
    print("These libraries handle geographic data and shapes.")
    print("Installation recommended via Conda: conda install geopandas")
    print("Alternatively, using pip: pip install geopandas shapely")
    print("(Pip installation might require additional system dependencies like GDAL/GEOS/PROJ)")
    exit(1)

wd_url = 'https://www.wikidata.org/wiki/'
url_base = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&ids='


def get_entities(query_ids):
    ids_joined = '|'.join(query_ids)
    req = urllib.request.Request(url_base + ids_joined)
    req.add_header('User-Agent', 'CoordinateFilter/0.1 (https://www.wikidata.org/wiki/User:Jamie7687)')
    # req.add_header('Accept-Encoding', 'gzip')
    time.sleep(1)
    result = urllib.request.urlopen(req)
    result_json = json.loads(result.read())
    if result_json['success'] == 1:
        return result_json['entities']
    else:
        raise Exception('wbgetentities call failed')

# common types among "implausible" coordinate items    
filter_types = set(['Q23442', 'Q207524', 'Q184358', 'Q190107', 'Q28337', 'Q503269', 'Q1402592', 'Q7944', 'Q852190', 'Q3002150',
                'Q963729', 'Q749622', 'Q211748', 'Q783953', 'Q27020041', 'Q366301', 'Q133156', 'Q26213387', 'Q1261499',
                'Q178561', 'Q19953632', 'Q332602', 'Q11702690', 'Q29102902', 'Q744913', 'Q165', 'Q813672', 'Q192611', 'Q570554',
                'Q14660', 'Q9319988', 'Q3305213', 'Q785020', 'Q191992', 'Q34198935', 'Q1310961', 'Q620225', 'Q39594', 'Q2360219',
                'Q674775', 'Q105999', 'Q11446', 'Q41767843', 'Q2811', 'Q3487904', 'Q21507948', 'Q13406463', 'Q1357601',
                'Q3024240', 'Q4164871', 'Q17018380', 'Q188055', 'Q28716292', 'Q1377943', 'Q119253', 'Q55436365', 'Q123695',
                'Q33837', 'Q1229765', 'Q1795675', 'Q2362867', 'Q1190554', 'Q134851', 'Q1140477', 'Q32099', 'Q24529780',
                'Q57833747', 'Q41982239', 'Q28966115', 'Q55193679', 'Q7888495', 'Q37901', 'Q7843791', 'Q213283', 'Q1161185',
                'Q3917681', 'Q997267', 'Q29898672', 'Q1069932', 'Q96251935', 'Q1210950', 'Q39715', 'Q187223', 'Q47781032'])

items = json.load(open('items.json'))

filtered_count = 0
deprecated_count = 0

unfiltered_counter = Counter()

# items not filtered will go into one of these, depending on whether flipping coordinates resolves the issue
flippable = {}
other = []

def is_filtered_type(obj):
    if 'claims' in obj and 'P31' in obj['claims']:
        types = [c['mainsnak']['datavalue']['value']['id'] for c in obj['claims']['P31']]
        for t in types:
            if t in filter_types:
                return True
        unfiltered_counter.update(types)
        return False

def is_deprecated(obj):
    """
    Returns True if any potentially conflicting claims are marked deprecated.
    """
    # TODO Don't return True unless ignoring the specific deprecated claim actually removes the conflict
    if 'P17' in obj['claims']:
        p17_ranks = [c['rank'] for c in obj['claims']['P17']]
        if 'deprecated' in p17_ranks:
            return True
    if 'P625' in obj['claims']:
        p625_ranks = [c['rank'] for c in obj['claims']['P625']]
        if 'deprecated' in p625_ranks:
            return True
    return False

def get_country_for_coords(lat, long):
    pass

def workable_flips(obj):
    try:
        claimed_country = obj['claims']['P17'][0]['mainsnak']['datavalue']['value']['id']
        coords_claims = obj['claims']['P625']
        workable_flips = []
        # TODO finish
        for cc in coords_claims:
            lat, long = cc['mainsnak']['datavalue']['value']['latitude'], cc['mainsnak']['datavalue']['value']['longitude']
            flips = [(lat, long), (-lat, long), (lat, -long), (-lat, -long), (long, lat), (-long, lat), (long, -lat), (-long, -lat)]
            for flip in flips:
                coords_country = get_country_for_coords(*flip)
                if coords_country == claimed_country:
                    print(flip, 'puts coordinates in claimed country!')
                    workable_flips.append(flip)
                else:
                    print(flip, 'does not put coords in claimed country')
        if len(workable_flips) > 0:
            return workable_flips
        else:
            return None
    except KeyError:
        return None

for i in items:
    wf = workable_flips(items[i])

    if 'P376' in items[i]['claims']:
        print(i, 'may not be on Earth...')
        continue
    if 'P30' in items[i]['claims'] and items[i]['claims']['P30'][0]['mainsnak']['datavalue']['value']['id'] == 'Q51':
        print(i, 'is in Antarctica')
        continue
    if is_filtered_type(items[i]):
        filtered_count += 1
        continue
    elif is_deprecated(items[i]):
        deprecated_count += 1
        continue
    elif wf:
        flippable[i] = wf
    else:
        other.append(i)

print('Flippable:')
for i in flippable:
    print(wd_url + i)

print(f"Other: ({len(other)}):")
for i in other:
    print(wd_url + i)
print(f"Other: ({len(other)})")

mc50 = unfiltered_counter.most_common()[:50]
type_dict = {t[0]: t[1] for t in mc50}
print(type_dict)
type_json = get_entities(type_dict.keys())
for i in type_json:
    print(i,' ' * (24 - len(i)),type_json[i]['labels']['en']['value'])
