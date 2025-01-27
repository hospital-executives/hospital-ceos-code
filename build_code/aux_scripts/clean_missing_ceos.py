## load data
import pickle 
import pandas as pd

# paths
comp_dropped_path = '/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/derived/temp/comp_dropped.pkl'
comp_remaining_path = '/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/derived/temp/comp_remaining.feather'
comp_pairs_path = '/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/derived/temp/component_pairs.feather'

meta_dropped_path = '/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/derived/temp/meta_dropped.pkl'
meta_remaining_path = '/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/derived/temp/meta_remaining.feather'
meta_pairs_path = '/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/derived/temp/meta_pairs.feather'

with open(comp_dropped_path, "rb") as f:
    comp_dropped = pickle.load(f)

with open(meta_dropped_path, "rb") as f:
    meta_dropped = pickle.load(f)

comp_remaining = pd.read_feather(comp_remaining_path)
comp_pairs = pd.read_feather(comp_pairs_path)
meta_remaining = pd.read_feather(meta_remaining_path)
meta_pairs = pd.read_feather(meta_pairs_path)

comp_remaining_set = set(zip(comp_remaining["contact_id1"], comp_remaining["contact_id2"]))
meta_remaining_set = set(zip(meta_remaining["contact_id1"], meta_remaining["contact_id2"]))

comp_set = set(zip(comp_pairs["contact_id1"], comp_pairs["contact_id2"]))
meta_set = set(zip(meta_pairs["contact_id1"], meta_pairs["contact_id2"]))


#def find_tuples_with_id(id_to_match, id_set):
    #"""
    #Finds all tuples in the set where either id1 or id2 matches the provided ID.

    #Args:
    #- id_to_match (str): The ID to search for.
    #- id_set (set of tuples): The set of (id1, id2) tuples.

   # Returns:
    #- set: A set of tuples where either id1 or id2 matches the provided ID.
   # """
   # #return {tup for tup in id_set if id_to_match in tup}


#ids_from_all = find_tuples_with_id('1343896', comp_set)
#dropped_ids = find_tuples_with_id('1343896', comp_dropped)

#ids_from_all = find_tuples_with_id('1343896', meta_set)
#dropped_ids = find_tuples_with_id('1343896', meta_dropped)

# case 1 - 1343896 should be included but i am missing them

#ids_from_all_1 = find_tuples_with_id(id, comp_set)
#dropped_ids_1 = find_tuples_with_id(id, comp_dropped)

#ids_from_all_2 = find_tuples_with_id(id, meta_set)
#dropped_ids_2 = find_tuples_with_id(id, meta_dropped)

def find_ids_to_add(
    ids, comp_set, comp_dropped, meta_set, meta_dropped
):
    """
    Identifies IDs to add to a new set based on conditions.

    Args:
    - ids (set): Set of IDs to process.
    - comp_set (set of tuples): Set of all comp pairs.
    - comp_dropped (set of tuples): Set of dropped comp pairs.
    - meta_set (set of tuples): Set of all meta pairs.
    - meta_dropped (set of tuples): Set of dropped meta pairs.

    Returns:
    - set: IDs that satisfy the condition.
    """
    def find_tuples_with_id(id_to_match, id_set):
        """Finds all tuples with the given ID."""
        return {tup for tup in id_set if id_to_match in tup}

    # Initialize the resulting set
    new_ids = set()

    # Loop through each ID
    for id in ids:
        # Get tuples for comp and meta
        ids_from_all_1 = find_tuples_with_id_optimized(id, comp_map)
        dropped_ids_1 = find_tuples_with_id_optimized(id, comp_dropped_map)

        ids_from_all_2 = find_tuples_with_id_optimized(id, meta_map)
        dropped_ids_2 = find_tuples_with_id_optimized(id, meta_dropped_map)

        # Check condition
        if len(ids_from_all_1) - len(dropped_ids_1) == 0 and len(ids_from_all_2) - len(dropped_ids_2) == 0:
            new_ids.add(id)

    return new_ids


#new_ids = find_ids_to_add(meta_remaining_set, comp_set, comp_dropped, 
                         # meta_set, meta_dropped)
#cleaned_new_ids = new_ids - outlier_ids
# test 
#test = new_ids.union(final_ids)

from collections import defaultdict

def build_id_map(id_set):
    """Build a mapping of IDs to the tuples they appear in."""
    id_map = defaultdict(set)
    for id1, id2 in id_set:
        id_map[id1].add((id1, id2))
        id_map[id2].add((id1, id2))
    return id_map

comp_map = build_id_map(comp_set)
comp_dropped_map = build_id_map(comp_dropped.union(confirmed_diff).union(confirmed_same))
meta_map = build_id_map(meta_set)
meta_dropped_map = build_id_map(meta_dropped.union(confirmed_diff).union(confirmed_same))

def find_tuples_with_id_optimized(id_to_match, id_map):
    """Efficiently find tuples using a precomputed map."""
    return id_map.get(id_to_match, set())

with open('/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/derived/temp/missing_ids.txt', "r") as f:
    ids_set = set(line.strip() for line in f)

#unmissing = cleaned_new_ids.intersection(ids_set) # recovered 179/293 =>

#still_missing = ids_set - unmissing

def find_valid_tuples_optimized(still_missing, comp_set, comp_dropped, meta_set, meta_dropped):
    """
    Optimized function to find valid tuples for each ID in still_missing.

    Args:
    - still_missing (set): Set of IDs to process.
    - comp_set (set of tuples): Set of all comp pairs.
    - comp_dropped (set of tuples): Set of dropped comp pairs.
    - meta_set (set of tuples): Set of all meta pairs.
    - meta_dropped (set of tuples): Set of dropped meta pairs.

    Returns:
    - dict: Dictionary where each key is an ID from still_missing, and the value is a set of tuples
            that are valid (in ids_from_all but not in dropped_ids) for both comp and meta sets.
    """
    def normalize_tuple(tup):
        """Normalize a tuple so that ('ida', 'idb') == ('idb', 'ida')."""
        return tuple(sorted(tup))

    # Preprocess: Normalize all tuples once
    comp_set_normalized = {normalize_tuple(tup) for tup in comp_set}
    comp_dropped_normalized = {normalize_tuple(tup) for tup in comp_dropped}
    meta_set_normalized = {normalize_tuple(tup) for tup in meta_set}
    meta_dropped_normalized = {normalize_tuple(tup) for tup in meta_dropped}

    # Precompute maps for fast lookup
    def build_id_to_tuples_map(normalized_set):
        """Build a mapping from ID to the normalized tuples it appears in."""
        id_to_tuples = {}
        for id1, id2 in normalized_set:
            if id1 not in id_to_tuples:
                id_to_tuples[id1] = set()
            if id2 not in id_to_tuples:
                id_to_tuples[id2] = set()
            id_to_tuples[id1].add((id1, id2))
            id_to_tuples[id2].add((id1, id2))
        return id_to_tuples

    comp_map = build_id_to_tuples_map(comp_set_normalized)
    comp_dropped_map = build_id_to_tuples_map(comp_dropped_normalized)
    meta_map = build_id_to_tuples_map(meta_set_normalized)
    meta_dropped_map = build_id_to_tuples_map(meta_dropped_normalized)

    # Result dictionary
    result = {}

    # Process each ID
    for id in still_missing:
        # Get tuples for comp_set
        ids_from_all_1 = comp_map.get(id, set())
        dropped_ids_1 = comp_dropped_map.get(id, set())
        valid_comp_tuples = ids_from_all_1 - dropped_ids_1

        # Get tuples for meta_set
        ids_from_all_2 = meta_map.get(id, set())
        dropped_ids_2 = meta_dropped_map.get(id, set())
        valid_meta_tuples = ids_from_all_2 - dropped_ids_2

        # Combine results
        result[id] = valid_comp_tuples.union(valid_meta_tuples)

    return result

new_comp_dropped = comp_dropped.union(set(confirmed_diff)).union(set(confirmed_same))
new_meta_dropped = meta_dropped.union(set(confirmed_diff)).union(set(confirmed_same))

result2 = find_valid_tuples_optimized(remaining_ids, 
                                         comp_set, new_comp_dropped, meta_set, 
                                         new_meta_dropped)

all_ids = set(input_df['contact_uniqueid'])
filtered_data = {key: value for key, value in result2.items() if key in all_ids}

# problem case 1 - they are (mostly) in the cleaned data but are not showing up
empty_values = {key: value for key, value in filtered_data.items() if len(value) == 0}

non_empty =  {key: value for key, value in filtered_data.items() if len(value) != 0}

#remaining = {key: value for key, value in non_empty.items() if key not in cleaned_ids}

remaining_with_all_pairs = {
    key: value 
    for key, value in result.items() 
    if  not 
    all(tup in (confirmed_same).union((confirmed_diff)) for tup in value)
}

accounted_for = set(confirmed_graph.edges()).union(set(meta_graph.edges())
                                              ).union(confirmed_same).union(
                                                  confirmed_diff
                                              )
remaining_pairs = {
    key: [tup for tup in value if tup not in accounted_for]
    for key, value in result.items()
    if any(tup not in accounted_for for tup in value)
}

curr = {key: value for key, value in remaining.items() if len(value) == 2}

ids = {item for tup in {
('105802', '2310655'),
 ('105802', '2345189'),
 ('105802', '593855')
  } #, ('23662394', '673454')} #, } 
                 for item in tup}
#ids = ('122114', '1350600')
new_himss[new_himss['contact_uniqueid'].isin(ids)][['contact_uniqueid', 
                                                          'firstname', 'lastname',
                                                          'madmin',
                                                            'title_standardized', 
                                                            'entity_name', 
                                                            'entity_uniqueid',
                                                            'entity_address',
                                                            'entity_zip',
                                                            'entity_state',
                                                            'system_id',
                                                            'year', 'entity_type']]
