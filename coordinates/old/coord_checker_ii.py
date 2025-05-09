#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wikidata Coordinate Location Checker (GeoPandas Version)

This script checks Wikidata items listed on a specified page
(e.g., User:Pasleim/Implausible/coordinate) to see if their coordinate
location (P625) falls within the boundary of their stated country (P17).
It uses GeoPandas and the Natural Earth dataset for country boundaries,
mapping countries via ISO 3166-1 alpha-3 codes fetched from Wikidata (P298).

Steps:
1. Fetch QIDs from the specified Wikidata page using raw Wikitext parsing.
2. Fetch item details (label, P625 coordinate, P17 country QID/label) via SPARQL.
3. Fetch country ISO codes (P298) via SPARQL. Load Natural Earth boundaries
   using GeoPandas and map countries via ISO codes to get Shapely geometries.
4. Perform geospatial check (Point in Polygon) using Shapely.
5. Output results to the console, sorted by implausibility score (distance).
"""

import requests
import re
import time
import json
import math
from SPARQLWrapper import SPARQLWrapper, JSON

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

# --- Configuration ---
TARGET_PAGE_URL = "https://www.wikidata.org/wiki/User:Pasleim/Implausible/coordinate"
WIKIDATA_USERNAME = "Jamie7687" # Your username
USER_AGENT = f'WikidataCoordinateCheckerBot/1.1-geopandas (User:{WIKIDATA_USERNAME}) Python/{requests.__version__}' # Updated agent
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
SPARQL_BATCH_SIZE = 100
# Batch size for ISO code lookup (can often be larger)
ISO_CODE_BATCH_SIZE = 200
SPARQL_DELAY = 1.0
# No GeoJSON delay needed now
REQUEST_TIMEOUT = 60
DETAILED_OUTPUT_LIMIT = 25
PROCESS_QID_LIMIT = None # Set to integer for testing, None for all


# --- Step 1: Fetch QIDs ---
# (Function get_qids_from_wikitext remains the same as before)
def get_qids_from_wikitext(page_url, username):
    """Fetches raw Wikitext and extracts QIDs."""
    qids = set()
    raw_url = f"{page_url}?action=raw"
    print(f"Fetching raw Wikitext from: {raw_url}")
    headers = {'User-Agent': USER_AGENT}
    try:
        response = requests.get(raw_url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        wikitext = response.text
        found_qids = re.findall(r'\[\[(Q[0-9]+)\]\]', wikitext)
        qids.update(found_qids)
        print(f"Found {len(qids)} unique QIDs.")
        return sorted(list(qids))
    except requests.exceptions.RequestException as e:
        print(f"  Error fetching page {raw_url}: {e}")
        return []
    except Exception as e:
        print(f"  An unexpected error occurred during QID extraction: {e}")
        return []

# --- Step 2: Get Item Details (SPARQL) ---
# (Function get_wikidata_item_details remains the same as before)
def get_wikidata_item_details(qids, batch_size, username):
    """Fetches details (label, coordinate, country) for QIDs via SPARQL."""
    sparql = SPARQLWrapper(SPARQL_ENDPOINT)
    sparql.agent = USER_AGENT # Set User-Agent for SPARQLWrapper

    results = {}
    num_qids = len(qids)
    print(f"\n--- Step 2: Fetching Item Details ---")
    print(f"Fetching details for {num_qids} QIDs in batches of {batch_size}...")

    for i in range(0, num_qids, batch_size):
        batch_qids = qids[i:min(i + batch_size, num_qids)]
        print(f"  Processing SPARQL batch {i//batch_size + 1}/{math.ceil(num_qids/batch_size)} ({len(batch_qids)} QIDs)...")

        values_clause = " ".join([f"wd:{qid}" for qid in batch_qids])
        query = f"""
        SELECT ?item ?itemLabel ?coord ?country ?countryLabel WHERE {{
          VALUES ?item {{ {values_clause} }}
          OPTIONAL {{ ?item wdt:P625 ?coord . }}
          OPTIONAL {{ ?item wdt:P17 ?country . }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
        }}
        """
        try:
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            sparql.setTimeout(REQUEST_TIMEOUT + 10)
            query_results = sparql.query().convert()
            bindings = query_results.get("results", {}).get("bindings", [])

            for binding in bindings:
                qid = binding.get("item", {}).get("value", "").split('/')[-1]
                if not qid: continue

                label = binding.get("itemLabel", {}).get("value")
                coord_str = binding.get("coord", {}).get("value")
                country_qid = binding.get("country", {}).get("value", "").split('/')[-1]
                country_label = binding.get("countryLabel", {}).get("value")

                coordinate = None
                if coord_str:
                    # Regex allows for optional space after comma if wikidata changes format
                    match = re.match(r'Point\(\s*(?P<lon>[-+]?\d*\.?\d+)\s+(?P<lat>[-+]?\d*\.?\d+)\s*\)', coord_str)
                    if match:
                        try:
                            lon = float(match.group('lon'))
                            lat = float(match.group('lat'))
                            if -90 <= lat <= 90 and -180 <= lon <= 180:
                                coordinate = {'lat': lat, 'lon': lon}
                            else:
                                print(f"    Warning: Invalid lat/lon range for {qid}: ({lat}, {lon})")
                        except ValueError:
                             print(f"    Warning: Could not parse float from coordinate for {qid}: {coord_str}")

                if qid not in results:
                     results[qid] = {} # Initialize dict for QID

                # Simplistic merge - overwrite with latest binding for this QID in batch
                results[qid] = {
                    'label': label if label else results[qid].get('label'),
                    'coordinate': coordinate if coordinate else results[qid].get('coordinate'),
                    'country_qid': country_qid if country_qid else results[qid].get('country_qid'),
                    'country_label': country_label if country_label else results[qid].get('country_label')
                }

        except Exception as e:
            print(f"    Error during SPARQL query for batch starting at QID {batch_qids[0]}: {e}")
            for qid in batch_qids:
                 if qid not in results:
                    results[qid] = {'error': str(e)}

        time.sleep(SPARQL_DELAY)

    print(f"Finished fetching details. Processed {len(results)} QIDs.")
    return results


# --- Step 3 (Revised): Get Country Geometries via GeoPandas / Natural Earth ---

def get_iso_codes_for_countries(country_qids, batch_size, username):
    """SPARQL query to get ISO 3166-1 alpha-3 codes (P298) for country QIDs."""
    sparql = SPARQLWrapper(SPARQL_ENDPOINT)
    sparql.agent = USER_AGENT
    iso_mapping = {}
    num_qids = len(country_qids)
    print(f"  Querying SPARQL for ISO A3 codes (P298) for {num_qids} countries...")

    processed_batches = 0
    for i in range(0, num_qids, batch_size):
        batch_qids = country_qids[i:min(i + batch_size, num_qids)]
        processed_batches += 1
        if num_qids > batch_size: # Only print batch progress if multiple batches
             print(f"    ISO Code SPARQL Batch {processed_batches}/{math.ceil(num_qids/batch_size)}...")

        values_clause = " ".join([f"wd:{qid}" for qid in batch_qids])
        query = f"""
        SELECT ?country ?isoCode WHERE {{
          VALUES ?country {{ {values_clause} }}
          ?country wdt:P298 ?isoCode . # ISO 3166-1 alpha-3 code
        }}
        """
        try:
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            sparql.setTimeout(REQUEST_TIMEOUT + 10)
            query_results = sparql.query().convert()
            bindings = query_results.get("results", {}).get("bindings", [])

            for binding in bindings:
                qid = binding.get("country", {}).get("value", "").split('/')[-1]
                iso_code = binding.get("isoCode", {}).get("value")
                if qid and iso_code:
                    # Take the first ISO code found
                    if qid not in iso_mapping:
                        iso_mapping[qid] = iso_code.strip().upper() # Normalize code
        except Exception as e:
            print(f"      Error during SPARQL query for ISO codes batch: {e}")
        time.sleep(SPARQL_DELAY)

    print(f"  Found ISO A3 codes for {len(iso_mapping)} countries via SPARQL.")
    return iso_mapping

def get_country_geometries_geopandas(country_qids, batch_size, username):
    """
    Fetches country geometries using GeoPandas and Natural Earth,
    mapping via ISO 3166-1 alpha-3 codes fetched from Wikidata.
    Uses the modern way to load 'naturalearth_lowres'.
    """
    geometries = {}
    num_countries = len(country_qids)
    print(f"\n--- Step 3 (GeoPandas): Mapping {num_countries} unique countries ---")

    # 1. Get ISO A3 codes from Wikidata
    country_iso_mapping = get_iso_codes_for_countries(country_qids, batch_size, username)
    qids_with_iso = set(country_iso_mapping.keys())
    qids_without_iso = set(country_qids) - qids_with_iso
    if qids_without_iso:
        print(f"  Note: Could not find ISO A3 code (P298) on Wikidata for {len(qids_without_iso)} countries.")

    # 2. Load Natural Earth data (using the modern method)
    world = None
    print("  Loading Natural Earth dataset ('naturalearth_lowres')...")
    try:
        world = gpd.read_file("~/data/ne_110m_admin_0_countries/ne_110m_admin_0_countries.shp")

        # Ensure WGS84 CRS (EPSG:4326)
        if world.crs and world.crs.to_epsg() != 4326:
             print("    Reprojecting Natural Earth data to EPSG:4326 (WGS84)...")
             world = world.to_crs(epsg=4326)
        elif not world.crs:
             print("    Warning: Natural Earth dataset loaded without CRS information. Assuming EPSG:4326.")
             world = world.set_crs(epsg=4326, allow_override=True)

        # Find the correct ISO A3 column name
        iso_col = None
        possible_iso_cols = ['ADM0_A3', 'ISO_A3_EH', 'ISO_A3']
        for col in possible_iso_cols:
             if col in world.columns:
                  iso_col = col
                  break
        if not iso_col:
             print("    Error: Could not find a suitable ISO A3 column (e.g., 'ADM0_A3') in the Natural Earth dataset.")
             print(f"    Available columns: {world.columns.tolist()}")
             return {}

        print(f"    Using '{iso_col}' column from Natural Earth for matching.")

    except Exception as e:
        print(f"    Error loading or processing Natural Earth dataset: {e}")
        # Check for common error if data needs downloading but fails
        if "not found" in str(e).lower() or "unable to open" in str(e).lower():
             print("    This might happen if GeoPandas couldn't automatically download the dataset.")
             print("    Ensure you have an internet connection when running for the first time.")
             print("    Alternatively, manually download '110m Cultural Vectors' from Natural Earth Data website,")
             print("    unzip it, and use gpd.read_file('/path/to/your/downloaded/ne_110m_admin_0_countries.shp')")
        else:
             print("    Please ensure geopandas and its dependencies are correctly installed.")
        return {}

    # 3. Map QIDs to geometries via ISO code
    print(f"  Matching {len(qids_with_iso)} countries to Natural Earth geometries via ISO A3 code...")
    # ... (rest of the mapping logic remains the same) ...
    matched_count = 0
    skipped_no_match = 0
    for qid in qids_with_iso:
        iso_code = country_iso_mapping[qid]
        match = world[world[iso_col] == iso_code]

        if not match.empty:
            geom = match.geometry.iloc[0]
            if geom and not geom.is_empty:
                 geometries[qid] = geom
                 matched_count += 1
        else:
            skipped_no_match +=1

    print(f"Finished Step 3. Found geometries for {matched_count} countries via GeoPandas/Natural Earth.")
    if skipped_no_match > 0:
        print(f"  ({skipped_no_match} countries with ISO codes couldn't be matched to Natural Earth geometries).")
    return geometries

# --- Step 4: Check Coordinates & Score ---
# (Function check_coordinates remains the same as before, it just uses geometries from the new Step 3)
def check_coordinates(item_data, country_geometries):
    """Checks item coordinates against country geometries and calculates scores."""
    results = []
    print("\n--- Step 4: Checking Coordinates against Country Geometries ---")

    checked_count = 0
    outside_count = 0
    missing_prereq_count = 0
    error_count = 0
    invalid_geom_count = 0 # Tracks count where geometry.is_valid was false

    for qid, data in item_data.items():
        status = "Unknown"
        score = None

        if 'error' in data:
            status = f"Error in Step 2 ({data.get('error', 'Unknown error')})"
            error_count += 1
        elif not data.get('coordinate'):
            status = "Missing Coordinate (P625)"
            missing_prereq_count += 1
        elif not data.get('country_qid'):
            status = "Missing Country (P17)"
            missing_prereq_count += 1
        else:
            country_qid = data['country_qid']
            country_geom = country_geometries.get(country_qid)
            if not country_geom:
                # Distinguish between no ISO code found and ISO code not matching Natural Earth
                status = "Missing Country Geometry (No ISO code or NE match)"
                missing_prereq_count += 1
            else:
                # Geometry found, proceed with check
                try:
                    item_point = Point(data['coordinate']['lon'], data['coordinate']['lat'])
                    geom_to_check = country_geom

                    # Check validity (geopandas/shapely geometries should generally be valid)
                    if not geom_to_check.is_valid:
                        # Log invalid shapes found from Natural Earth (less common than from Commons)
                        print(f"  Warning: Geometry for country {country_qid} from Natural Earth is invalid. Attempting buffer(0).")
                        invalid_geom_count += 1
                        buffered_geom = geom_to_check.buffer(0)
                        if buffered_geom.is_valid and not buffered_geom.is_empty:
                             geom_to_check = buffered_geom
                        else:
                             print(f"    Buffer(0) failed for {country_qid}. Cannot check {qid}.")
                             status = "Invalid Country Geometry (NE)"


                    if status == "Unknown": # Proceed if geom is usable
                        # Perform check
                        if geom_to_check.contains(item_point):
                            status = "Inside"
                            score = 0.0
                        else:
                            status = "Outside"
                            score = geom_to_check.distance(item_point) # Score is distance in degrees
                            outside_count += 1
                        checked_count += 1

                except Exception as e:
                    print(f"    Error checking coordinate for {qid} against {country_qid}: {e}")
                    status = f"Error during check: {e}"
                    error_count += 1

        # Append result
        results.append({
            'qid': qid,
            'label': data.get('label', '[No Label]'),
            'coordinate': data.get('coordinate'),
            'country_qid': data.get('country_qid'),
            'country_label': data.get('country_label', '[No Country Label]'),
            'status': status,
            'score': score
        })

    print(f"Finished checking {len(item_data)} items.")
    print(f"  - Successfully compared geometry for: {checked_count} items")
    print(f"    - Inside boundary: {checked_count - outside_count}")
    print(f"    - Outside boundary: {outside_count}")
    print(f"  - Could not check (Missing Data/Shape/ISO/NE Match): {missing_prereq_count} items")
    if invalid_geom_count > 0:
        print(f"  - Encountered Invalid GeoShapes from Natural Earth: {invalid_geom_count} times (included in Error count)")
    print(f"  - Errors during processing (Step 2 or 4): {error_count + invalid_geom_count} items")

    # Sort results
    def sort_key(item):
        if item['status'] == 'Outside':
            return (0, -item['score'] if item['score'] is not None else 0)
        elif item['status'] == 'Inside':
            return (1, 0)
        else:
            return (2, item['status'])
    results.sort(key=sort_key)
    return results


# --- Main Execution ---
if __name__ == "__main__":
    print("Starting Wikidata Coordinate Checker (GeoPandas Version)...")
    start_time = time.time()

    # Step 1: Fetch QIDs
    print("--- Step 1: Fetching QIDs ---")
    qids_to_check = get_qids_from_wikitext(TARGET_PAGE_URL, WIKIDATA_USERNAME)

    if PROCESS_QID_LIMIT and len(qids_to_check) > PROCESS_QID_LIMIT:
        print(f"\nLimiting processing to the first {PROCESS_QID_LIMIT} QIDs found.")
        qids_to_check = qids_to_check[:PROCESS_QID_LIMIT]

    item_data = {}
    country_geometries = {}
    final_results = []

    if qids_to_check:
        # Step 2: Fetch Item Details
        item_data = get_wikidata_item_details(qids_to_check, SPARQL_BATCH_SIZE, WIKIDATA_USERNAME)

        if item_data:
            # Step 3: Fetch Country Geometries (GeoPandas version)
            unique_country_qids = set()
            for data in item_data.values():
                 if 'error' not in data and data.get('country_qid'):
                    unique_country_qids.add(data['country_qid'])

            if unique_country_qids:
                 # Call the new GeoPandas-based function
                 country_geometries = get_country_geometries_geopandas(
                     list(unique_country_qids),
                     ISO_CODE_BATCH_SIZE, # Use specific batch size for ISO lookup
                     WIKIDATA_USERNAME
                 )
            else:
                 print("\n--- Step 3: Skipping Geometry Mapping (No valid country QIDs found) ---")

            # Step 4: Check Coordinates
            # This function doesn't need to change, it uses the dict from Step 3
            final_results = check_coordinates(item_data, country_geometries)

            # Step 5: Output Results to Console
            # (This part remains the same - prints the final_results list)
            print("\n" + "="*40)
            print("--- Step 5: Final Report ---")
            print("="*40)

            outside_items = [item for item in final_results if item['status'] == 'Outside']
            other_issues = [item for item in final_results if item['status'] not in ['Inside', 'Outside']]

            if outside_items:
                print(f"\n--- Top {min(DETAILED_OUTPUT_LIMIT, len(outside_items))} Potential Mismatches (Status='Outside', sorted by score) ---")
                for i, item in enumerate(outside_items[:DETAILED_OUTPUT_LIMIT]):
                    coord_str = "None"
                    if item['coordinate']:
                        coord_str = f"Lat={item['coordinate']['lat']:.4f}, Lon={item['coordinate']['lon']:.4f}"
                    qid_link = f"https://www.wikidata.org/wiki/{item['qid']}"
                    country_qid_link = f"https://www.wikidata.org/wiki/{item['country_qid']}" if item['country_qid'] else "None"

                    print(f"\n{i + 1}. QID: {qid_link} ({item['label']})")
                    print(f"   Coord: {coord_str}")
                    print(f"   Country: {country_qid_link} ({item['country_label']})")
                    print(f"   Score (Distance in degrees): {item['score']:.4f}")
            else:
                 print("\n--- Potential Mismatches (Status='Outside') ---")
                 print("No items found with status 'Outside'.")


            if other_issues:
                 print(f"\n--- Summary of Items Not Fully Checked ({len(other_issues)} items) ---")
                 issues_by_status = {}
                 for item in other_issues:
                      status = item['status']
                      issues_by_status[status] = issues_by_status.get(status, 0) + 1
                 for status in sorted(issues_by_status.keys()):
                       print(f"- {status}: {issues_by_status[status]} items")
            else:
                  print("\n--- Summary of Items Not Fully Checked ---")
                  print("All items were either 'Inside' or 'Outside'.")

        else:
             print("\nCould not fetch item details (Step 2), processing halted.")
    else:
        print("\nCould not fetch QIDs (Step 1), script cannot run.")

    end_time = time.time()
    print("\n" + "="*40)
    print(f"Script finished in {end_time - start_time:.2f} seconds.")
    print("="*40)