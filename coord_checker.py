#!/usr/bin/env python3
"""script to check items with potentially bad coordinates"""

import json

items_url = 'https://www.wikidata.org/w/index.php?title=User:Pasleim/Implausible/coordinate&action=raw'

def get_qids(wikitext):
    """given some wikitext, get the QIDs linked therein"""
    pass

def get_items(qids):
    """given a list of QIDs, get the items"""
    pass

def get_coords(item):
    """given item JSON, retrieve coordinates (P625) claims"""
    pass

def coords_in_country(coords, country):
    """given a set of coordinates and a country, return whether those coordinates are within that country"""
    pass