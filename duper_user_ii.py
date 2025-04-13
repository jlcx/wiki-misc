#!/usr/bin/env python3
""""Gets items created by a user, and attempts to find duplicates and suggest merges."""

# developed with Google Gemini 2.5 Pro 

import requests
import csv
from datetime import datetime
from collections import defaultdict
import time
import io # To generate CSV in memory
import re # For label normalization

# --- Configuration ---
# IMPORTANT: Change this User-Agent! Follow Wikimedia's User-Agent policy:
# https://meta.wikimedia.org/wiki/User-Agent_policy
USER_AGENT = "DuperUser/0.4 (https://www.wikidata.org/wiki/User:Jamie7687)" 
WIKIDATA_API_ENDPOINT = "https://www.wikidata.org/w/api.php"
LANGUAGE = "en" # Primary language for labels and search
SEARCH_LIMIT = 7 # Max number of search results per item to check (adjust as needed)
# Consider adding fuzzy matching later if exact label matching is too strict
# from thefuzz import fuzz # Example library

# --- 1. Fetch Items Created by User (Using MediaWiki API User Contributions) ---
# (Function fetch_items_by_user remains the same as the previous version)
def fetch_items_by_user(username):
    """
    Fetches the QIDs of items created by a specific user using the MediaWiki API (list=usercontribs).
    Returns a dictionary {qid: {'creation_date': datetime, 'pageid_contrib': int}}.
    """
    print(f"Searching for items created by user: {username} via MediaWiki API...")
    items = {}
    api_params = {
        "action": "query", "list": "usercontribs", "ucuser": username,
        "ucprop": "title|timestamp|ids", "uclimit": "max", "ucnamespace": "0",
        "ucshow": "new", "ucdir": "newer", "format": "json", "formatversion": "2"
    }
    headers = {'User-Agent': USER_AGENT}
    count = 0
    max_items_limit = 10000 # Safety limit

    while count < max_items_limit:
        try:
            print(f"  Fetching API batch (user items, current total: {count})...")
            response = requests.get(WIKIDATA_API_ENDPOINT, headers=headers, params=api_params, timeout=60)
            response.raise_for_status()
            data = response.json()
            contributions = data.get("query", {}).get("usercontribs", [])
            if not contributions: break
            batch_found = 0
            for contrib in contributions:
                if contrib.get("title", "").startswith("Q") and "pageid" in contrib:
                    qid = contrib["title"]
                    if qid in items: continue
                    pageid = contrib["pageid"]
                    timestamp_str = contrib.get("timestamp")
                    creation_date = None
                    if timestamp_str:
                        try: creation_date = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        except ValueError: pass # Ignore parse errors
                    items[qid] = {'creation_date': creation_date, 'pageid_contrib': pageid}
                    count += 1; batch_found += 1
            print(f"  Found {batch_found} new user items in this batch.")
            if "continue" in data: api_params.update(data["continue"]); time.sleep(0.5)
            else: break
            if count >= max_items_limit: print(f"Warning: Reached item limit ({max_items_limit})."); break
        except requests.exceptions.RequestException as e: print(f"Error fetching user items: {e}"); break
        except Exception as e: print(f"Error processing user item response: {e}"); break
    print(f"Found a total of {len(items)} items created by the user via API.")
    return items

# --- 2. Fetch Item Details (Slightly modified to handle single or multiple QIDs) ---
def fetch_item_details_batch(qids_to_fetch):
    """
    Fetches details (label, description, P31, pageid) for a list of QIDs using wbgetentities.
    Returns a dictionary {qid: {'label': str, 'description': str, 'p31': list, 'pageid': int}}.
    Handles cases where some QIDs might be missing or lack data.
    """
    if not qids_to_fetch:
        return {}
        
    qids_list = list(qids_to_fetch) # Ensure it's a list
    # print(f"Fetching details for {len(qids_list)} items using wbgetentities...")
    item_details = {}
    batch_size = 50 
    
    for i in range(0, len(qids_list), batch_size):
        batch_qids = qids_list[i:i+batch_size]
        # print(f"  Processing wbgetentities batch {i//batch_size + 1}/{(len(qids_list) + batch_size - 1)//batch_size} ({len(batch_qids)} items)")
        params = {
            'action': 'wbgetentities', 'ids': '|'.join(batch_qids), 'format': 'json',
            'props': 'labels|descriptions|claims|info', 'languages': LANGUAGE, 'languagefallback': '1'
        }
        headers = {'User-Agent': USER_AGENT}
        
        try:
            response = requests.get(WIKIDATA_API_ENDPOINT, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            entities = data.get('entities', {})
            
            for qid, entity_data in entities.items():
                if 'missing' in entity_data: continue # Skip missing items
                
                label = entity_data.get('labels', {}).get(LANGUAGE, {}).get('value')
                description = entity_data.get('descriptions', {}).get(LANGUAGE, {}).get('value')
                p31_claims = entity_data.get('claims', {}).get('P31', [])
                p31_values = []
                for claim in p31_claims:
                    mainsnak = claim.get('mainsnak', {})
                    if mainsnak.get('snaktype') == 'value':
                        datavalue = mainsnak.get('datavalue', {})
                        if datavalue.get('type') == 'wikibase-entityid':
                            p31_qid = datavalue.get('value', {}).get('id')
                            if p31_qid: p31_values.append(p31_qid)
                            
                pageid = entity_data.get('pageid')

                # Only store if we have the essential pageid
                if pageid is not None:
                    item_details[qid] = {
                        'label': label, 'description': description,
                        'p31': sorted(list(set(p31_values))), 'pageid': pageid
                    }
            time.sleep(0.2) # Shorter sleep ok for wbgetentities usually
        except requests.exceptions.RequestException as e: print(f"Error fetching details (wbgetentities) for batch {batch_qids}: {e}")
        except Exception as e: print(f"Error processing details (wbgetentities) response for batch {batch_qids}: {e}")

    # print(f"Fetched details for {len(item_details)} items.")
    return item_details

# --- 3. Search for Potential Duplicates using wbsearchentities ---
def search_potential_duplicates(label_to_search):
    """
    Uses wbsearchentities to find items with a similar label across all Wikidata.
    Returns a list of QIDs found.
    """
    if not label_to_search:
        return []
        
    params = {
        "action": "wbsearchentities",
        "search": label_to_search,
        "language": LANGUAGE,
        "uselang": LANGUAGE, # Use language for response too
        "type": "item",
        "limit": SEARCH_LIMIT,
        "format": "json",
        "formatversion": "2"
    }
    headers = {'User-Agent': USER_AGENT}
    candidate_qids = []
    
    try:
        # print(f"  Searching for label: '{label_to_search}'...")
        response = requests.get(WIKIDATA_API_ENDPOINT, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        results = data.get("search", [])
        
        for result in results:
            qid = result.get("id")
            if qid and qid.startswith("Q"):
                candidate_qids.append(qid)
                
    except requests.exceptions.RequestException as e:
        print(f"  Error during wbsearchentities for '{label_to_search}': {e}")
    except Exception as e:
        print(f"  Error processing wbsearchentities response for '{label_to_search}': {e}")
        
    # print(f"  Found {len(candidate_qids)} candidates via search.")
    return candidate_qids

# --- 4. Normalize Label ---
def normalize_label(label):
    """Applies robust normalization to labels for comparison."""
    if not label: return ""
    text = label.lower()
    text = re.sub(r'[^\w\s]', '', text) # Remove punctuation
    text = re.sub(r'\s+', ' ', text).strip() # Collapse whitespace
    return text

# --- 5. Compare User Item with Candidates and Identify Duplicates ---
def compare_and_find_duplicates(user_item_qid, user_item_details, candidate_qids):
    """
    Compares a user's item with potential duplicate candidates found via search.
    Fetches details for candidates and performs comparison based on label, P31, and pageid.
    Returns a list of merge pairs: (user_item_qid, older_community_item_qid).
    """
    
    identified_merge_pairs = []
    if not candidate_qids or user_item_details.get('pageid') is None or not user_item_details.get('label'):
        return [] # Cannot compare without user item's pageid and label

    user_pageid = user_item_details['pageid']
    user_norm_label = normalize_label(user_item_details['label'])
    # Ensure P31 is a tuple for comparison
    user_p31_tuple = tuple(user_item_details.get('p31', [])) 

    # Fetch details for the candidates found by search
    # print(f"  Fetching details for {len(candidate_qids)} candidates...")
    candidate_details = fetch_item_details_batch(candidate_qids)
    # print(f"  Got details for {len(candidate_details)} candidates.")

    # Compare user item with each candidate that we got details for
    for cand_qid, cand_data in candidate_details.items():
        
        # Basic checks for the candidate
        if cand_data.get('pageid') is None or not cand_data.get('label'):
            # print(f"    Skipping candidate {cand_qid} (missing pageid or label)")
            continue 
            
        cand_pageid = cand_data['pageid']
        cand_norm_label = normalize_label(cand_data['label'])
        cand_p31_tuple = tuple(cand_data.get('p31', []))

        # --- Core Comparison Logic ---
        
        # 1. Age Check: Candidate must be older (lower pageid)
        if not (cand_pageid < user_pageid):
            # print(f"    Skipping candidate {cand_qid} (not older: P{cand_pageid} vs user P{user_pageid})")
            continue

        # 2. Label Check: Require exact match on normalized labels
        #    (wbsearchentities might return fuzzy matches, so we confirm here)
        #    TODO: Consider adding fuzzy matching here if needed (e.g., using thefuzz)
        #    if fuzz.ratio(user_norm_label, cand_norm_label) < 95: # Example fuzzy threshold
        if user_norm_label != cand_norm_label:
            # print(f"    Skipping candidate {cand_qid} (Normalized label mismatch: '{cand_norm_label}' != '{user_norm_label}')")
            continue
            
        # 3. Instance Of (P31) Check: Require identical P31 sets (strict!)
        #    TODO: Relax this? Check for overlap? Ignore P31? Depends on use case.
        if user_p31_tuple != cand_p31_tuple:
            # print(f"    Skipping candidate {cand_qid} (P31 mismatch: {cand_p31_tuple} != {user_p31_tuple})")
            continue
            
        # --- If all checks pass, it's a potential duplicate ---
        print(f"  >>> Potential Duplicate Found for {user_item_qid}:")
        print(f"      User Item (Newer): {user_item_qid} (P:{user_pageid}, L:'{user_item_details['label']}', P31:{user_p31_tuple})")
        print(f"      Community Item (Older): {cand_qid} (P:{cand_pageid}, L:'{cand_data['label']}', P31:{cand_p31_tuple})")
        
        # Add pair: (newer_qid, older_qid)
        identified_merge_pairs.append((user_item_qid, cand_qid))
        
        # Optimization: If we find a good match, maybe stop searching for this user_item? Optional.
        # break 
            
    return identified_merge_pairs


# --- 6. Generate QuickStatements CSV (Unchanged) ---
# (Function generate_quickstatements_csv remains the same)
def generate_quickstatements_csv(duplicate_pairs):
    """
    Generates a CSV formatted string for QuickStatements v1 to merge items.
    """
    if not duplicate_pairs:
        print("No duplicate pairs to generate CSV for.")
        return None
    print("Generating CSV for QuickStatements...")
    output = io.StringIO()
    writer = csv.writer(output, delimiter='\t', lineterminator='\n', quoting=csv.QUOTE_NONE) 
    for newer_qid, older_qid in duplicate_pairs:
        writer.writerow(['MERGE', newer_qid, older_qid])
    csv_content = output.getvalue()
    output.close()
    print("CSV generated (preview):")
    print("-" * 20)
    lines = csv_content.strip().split('\n'); preview_lines = 5
    for line in lines[:preview_lines]: print(line)
    if len(lines) > preview_lines: print(f"... ({len(lines) - preview_lines} more lines)")
    print("-" * 20)
    return csv_content


# --- Main Script ---
if __name__ == "__main__":
    target_username = input("Enter the Wikidata username whose created items you want to check against the community: ") 

    if not target_username:
        print("Username not provided. Exiting.")
    else:
        start_time = time.time()
        
        # 1. Fetch items created by the target user
        user_items_initial = fetch_items_by_user(target_username) 
        
        if not user_items_initial:
            print("\nNo items found for this user to process.")
        else:
            user_qids = list(user_items_initial.keys())
            print(f"\nFetching details for {len(user_qids)} items created by {target_username}...")
            user_items_details = fetch_item_details_batch(user_qids)
            
            # Filter user items that lack essential details for searching/comparison
            valid_user_items = {
                qid: data for qid, data in user_items_details.items()
                if data.get('pageid') is not None and data.get('label') is not None
            }
            if len(valid_user_items) < len(user_items_details):
                 print(f"Warning: Excluding {len(user_items_details) - len(valid_user_items)} user items lacking PageID or Label in '{LANGUAGE}'.")

            print(f"\nNow checking {len(valid_user_items)} user items against all of Wikidata...")
            all_merge_pairs = []
            processed_count = 0
            
            # --- Main Loop: Check each user item ---
            for user_qid, user_details in valid_user_items.items():
                processed_count += 1
                print(f"\n[{processed_count}/{len(valid_user_items)}] Checking user item: {user_qid} ('{user_details.get('label', 'N/A')}')")
                
                # 3. Search for potential duplicates based on label
                # Use the user item's primary label for the search
                candidate_qids_from_search = search_potential_duplicates(user_details['label'])
                
                # Filter out the item itself from search results
                filtered_candidates = [qid for qid in candidate_qids_from_search if qid != user_qid]
                
                if not filtered_candidates:
                    print(f"  No potential duplicate candidates found via search for {user_qid}.")
                    time.sleep(0.5) # Pause even if no results to avoid hammering API
                    continue

                print(f"  Found {len(filtered_candidates)} potential candidates via search. Comparing details...")
                
                # 5. Compare user item with candidates
                merge_pairs_found = compare_and_find_duplicates(user_qid, user_details, filtered_candidates)
                
                if merge_pairs_found:
                    all_merge_pairs.extend(merge_pairs_found)
                    
                # Be nice to the API - pause between checking each user item
                time.sleep(1.0) # Adjust sleep time as needed (1 sec is conservative)

            # --- End Main Loop ---

            end_time = time.time()
            print(f"\n--- Check Complete ---")
            print(f"Processed {len(valid_user_items)} user items in {end_time - start_time:.2f} seconds.")
            
            # 6. Generate QuickStatements CSV for all found pairs
            if all_merge_pairs:
                print(f"\nFound a total of {len(all_merge_pairs)} potential duplicate pairs requiring merges.")
                # Remove potential duplicate pairs (e.g., if search found A->B and later B->A somehow)
                unique_merge_pairs = list(set(all_merge_pairs))
                if len(unique_merge_pairs) != len(all_merge_pairs):
                    print(f"Removed {len(all_merge_pairs) - len(unique_merge_pairs)} duplicate merge suggestions.")
                
                qs_csv = generate_quickstatements_csv(unique_merge_pairs)
                if qs_csv:
                    # Save to file
                    safe_username = target_username.replace(" ", "_").replace("/", "_")
                    filename = f"qs_community_duplicates_{safe_username}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    try:
                        with open(filename, 'w', newline='', encoding='utf-8') as f:
                            f.write(qs_csv)
                        print(f"\nQuickStatements CSV file saved as: {filename}")
                        print("\n*********************************************************************")
                        print("IMPORTANT! Carefully review the CSV file before uploading.")
                        print("Verify each suggested merge between the user's item and the existing community item.")
                        print("Ensure the community item is genuinely older and the correct target.")
                        print("*********************************************************************")
                    except IOError as e:
                        print(f"\nError saving the CSV file: {e}")
            else:
                print("\nNo potential duplicates needing merges were found based on the criteria.")

        print("\nScript finished.")