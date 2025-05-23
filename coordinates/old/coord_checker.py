#!/usr/bin/env python3
"""script to check items with potentially bad coordinates"""

import json
import re

items_url = 'https://www.wikidata.org/w/index.php?title=User:Pasleim/Implausible/coordinate&action=raw'

def get_qids(wikitext):
    """given some wikitext, get the QIDs linked therein"""
    results = []
    matches = re.findall(r'\[\[[^\]]*\]\]', wikitext)
    for match in matches:
        stripped = match.strip('[]')
        if "|" in stripped:
            stripped = stripped.split("|")[0]
        results.append(stripped)
    return results

def get_items(qids):
    """given a list of QIDs, get the items"""
    pass

def get_coords(item):
    """given item JSON, retrieve coordinates (P625) claims"""
    pass

def get_wp_coords(sitelink):
    """given a (probably) Wikipedia sitelink, get item coordinates from the article"""
    # if they're the same, they may also need to be corrected
    # if they're different, hopefully they are correct already
    pass

def coords_in_country(coords, country):
    """given a set of coordinates and a country, return whether those coordinates are within that country"""
    pass

def try_flip(coords, country):
    """given a set of coordinates and a country, determine whether flipping them (N/S and/or E/W) puts them in the country of interest"""
    pass

def search_item_history(item):
    """search item history for related problematic edits (e.g. coordinates or country changed)"""
    pass

def item_added_date(item):
    """search history of items list and determine when the item was most recently added"""
    pass
