import json

items = json.load(open('items.json'))
qids = [l.strip() for l in open('senegalese_hospitals.txt').readlines()]

def get_coordinates(obj):
    """
    Given Wikidata item JSON, returns coordinates
    """
    try:
        return obj['claims']['P625'][0]['mainsnak']['datavalue']['value']
    except:
        return None
    
print('qid,P625,-P625')
for qid in qids:
    coords = get_coordinates(items[qid])
    print(f"{qid},@{coords['longitude']}/{coords['latitude']},@{coords['latitude']}/{coords['longitude']}")
