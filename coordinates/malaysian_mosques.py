import json

items = json.load(open('items.json'))

def get_coordinates(obj):
    """
    Given Wikidata item JSON, returns coordinates
    """
    try:
        return obj['claims']['P625'][0]['mainsnak']['datavalue']['value']
    except:
        return None
    
malaysian_mosques = []
malaysian_mosque_count = 0
m_m_exceptions = []

for i in items:
    try:
        malaysian = 'P17' in items[i]['claims'] and items[i]['claims']['P17'][0]['mainsnak']['datavalue']['value']['id'] == 'Q833'
        mosque = 'P31' in items[i]['claims'] and items[i]['claims']['P31'][0]['mainsnak']['datavalue']['value']['id'] == 'Q32815'
        if malaysian and mosque:
            malaysian_mosque_count += 1
            malaysian_mosques.append(i)
    except Exception as e:
        m_m_exceptions.append((i, e))

print(malaysian_mosque_count, 'Malaysian mosques')

print(m_m_exceptions)

print('qid,P625,-P625')
for qid in malaysian_mosques:
    try:
        coords = get_coordinates(items[qid])
        print(f"{qid},@{-coords['latitude']}/{coords['longitude']},@{coords['latitude']}/{coords['longitude']}")
    except:
        pass

