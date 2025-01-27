from joblib import Parallel, delayed
import pandas as pd
import Levenshtein
import jellyfish
from itertools import combinations
import pandas as pd
import jellyfish
import Levenshtein 
from itertools import combinations
from collections import defaultdict

# BLOCKWISE COMPARISON FUNCTIONS
def parallel_blocks(group):
    return find_common_blocks(group)

def find_common_blocks(sub_df, first_meta=None):

    # Grouping by 'contact_uniqueid' and collecting sets in a single pass
    grouped = sub_df.groupby('contact_uniqueid').agg({
        'title_standardized': set,
        'entity_name': set,
        'entity_uniqueid': set,
        'system_id': set,
        'entity_address': set,
        'entity_zip_five': set
    })
    
    # Finding common elements in the sets
    if len(grouped) > 1:
        common_titles = set.intersection(*grouped['title_standardized'])
        common_names = set.intersection(*grouped['entity_name'])
        common_entity_ids = set.intersection(*grouped['entity_uniqueid'])
        common_system_ids = set.intersection(*grouped['system_id'])
        common_adds = set.intersection(*grouped['entity_address'])
        common_zips = set.intersection(*grouped['entity_zip_five'])
    else:
        common_titles = common_names = common_entity_ids = common_system_ids = \
            common_adds = common_zips = set()
    # Common data to return in both cases
    common_data = {
        'contact_uniqueid': list(sub_df['contact_uniqueid'].unique()),
        'entity_state': list(sub_df['entity_state'].unique()),
        'entity_name': list(sub_df['entity_name'].unique()),
        'entity_uniqueid': list(sub_df['entity_uniqueid'].unique()),
        'entity_address': list(sub_df['entity_address'].unique()),
        'system_id': list(sub_df['system_id'].unique()),
        'entity_zip_five': list(sub_df['entity_zip_five'].unique()),
        'common_titles': list(common_titles),
        'common_names': list(common_names),
        'common_entity_ids': list(common_entity_ids),
        'common_system_ids': list(common_system_ids),
        'common_adds': list(common_adds),
        'common_zips': list(common_zips)
    }

    if first_meta:
        common_data['first_component'] = list(sub_df['first_component'].unique())
    else:
        common_data['first_meta'] = list(sub_df['first_meta'].unique())

    return pd.Series(common_data)

# PAIRWISE COMPARISON FUNCTIONS
def compute_shared_attributes(id1, id2, attribute_dicts, name_pairs_set, 
                              meta_pairs_set, sub_df):
    
    # Retrieve attributes for id1 and id2 once
    attrs1 = {attr: attribute_dicts[attr][id1] for attr in attribute_dicts}
    attrs2 = {attr: attribute_dicts[attr][id2] for attr in attribute_dicts}

    # Compute shared attributes
    shared_titles = attrs1['title'] & attrs2['title']
    shared_names = attrs1['name'] & attrs2['name']
    shared_entity_ids = attrs1['entity_id'] & attrs2['entity_id']
    shared_system_ids = attrs1['system_id'] & attrs2['system_id']
    shared_addresses = attrs1['address'] & attrs2['address']
    shared_zips = attrs1['zip'] & attrs2['zip']
    shared_states = attrs1['state'] & attrs2['state']
    distinct_state_count = len(attrs1['state'] | attrs2['state'])

    # Helper function to compute distances and similarities
    def compute_distances(s1, s2):
        if not s1 or not s2:
            return None, None
        lev_distance = Levenshtein.distance(s1, s2)
        jw_similarity = jellyfish.jaro_winkler_similarity(s1, s2)
        return lev_distance, jw_similarity

    # Compute distances and similarities for first and last names
    firstname_lev_distance, firstname_jw_distance = compute_distances(attrs1['firstname'], attrs2['firstname'])
    old_firstname_lev_distance, old_firstname_jw_distance = compute_distances(attrs1['old_firstname'], attrs2['old_firstname'])
    lastname_lev_distance, lastname_jw_distance = compute_distances(attrs1['lastname'], attrs2['lastname'])
    old_lastname_lev_distance, old_lastname_jw_distance = compute_distances(attrs1['old_lastname'], attrs2['old_lastname'])

    same_first_component = int(attrs1['first_component'] == attrs2['first_component'])

    # Check if names and meta codes are in the same row
    name_in_same_row_firstname = (attrs1['firstname'], attrs2['firstname']) in name_pairs_set
    name_in_same_row_old_firstname = (attrs1['old_firstname'], attrs2['old_firstname']) in name_pairs_set

    first_meta_codes1 = attrs1['first_meta']
    first_meta_codes2 = attrs2['first_meta']
    meta_in_same_row = any((m1, m2) in meta_pairs_set for m1 in first_meta_codes1 for m2 in first_meta_codes2)

    # Extract subset of sub_df for the two IDs once
    sub_df_ids = sub_df[sub_df['contact_uniqueid'].isin([id1, id2])]

    # Compute gender-related flags
    gender_set = set(sub_df_ids['gender'])
    both_F_and_M_present = 'F' in gender_set and 'M' in gender_set

    # Compute lastname count flag
    lastname_counts = sub_df_ids['lastname_count']
    frequent_lastname_flag = any(count > 8 for count in lastname_counts)

    # Prepare the result
    result = {
        'last_meta': sub_df_ids['last_meta'].iloc[0] if not sub_df_ids['last_meta'].empty else None,
        'first_component': sub_df_ids['first_component'].iloc[0] if not sub_df_ids['first_component'].empty else None,
        'contact_id1': id1,
        'contact_id2': id2,
        'shared_titles': list(shared_titles),
        'shared_names': list(shared_names),
        'shared_entity_ids': list(shared_entity_ids),
        'shared_system_ids': list(shared_system_ids),
        'shared_addresses': list(shared_addresses),
        'shared_zips': list(shared_zips),
        'shared_states': list(shared_states),
        'distinct_state_count': distinct_state_count,
        'firstname_lev_distance': firstname_lev_distance,
        'old_firstname_lev_distance': old_firstname_lev_distance,
        'lastname_lev_distance': lastname_lev_distance,
        'old_lastname_lev_distance': old_lastname_lev_distance,
        'firstname_jw_distance': firstname_jw_distance,
        'old_firstname_jw_distance': old_firstname_jw_distance,
        'lastname_jw_distance': lastname_jw_distance,
        'old_lastname_jw_distance': old_lastname_jw_distance,
        'same_first_component': same_first_component,
        'name_in_same_row_firstname': name_in_same_row_firstname,
        'name_in_same_row_old_firstname': name_in_same_row_old_firstname,
        'meta_in_same_row': meta_in_same_row,
        'both_F_and_M_present': both_F_and_M_present,
        'frequent_lastname_flag': frequent_lastname_flag,
        'max_lastname_count_id1': attrs1['max_lastname_count'],
        'max_lastname_count_id2': attrs2['max_lastname_count']
    }

    return result

def find_pairwise_shared_attributes(sub_df, name_pairs_set, meta_pairs_set):

    contact_ids = sub_df['contact_uniqueid'].unique()

    if len(contact_ids) < 2:
        return pd.DataFrame([], columns=[
            'last_meta', 'first_component', 'contact_id1', 'contact_id2', 
            'shared_titles', 'shared_names', 'shared_entity_ids', 'shared_system_ids', 
            'shared_addresses', 'shared_zips', 'shared_states', 'distinct_state_count',
            'firstname_lev_distance', 'old_firstname_lev_distance', 
            'lastname_lev_distance', 'old_lastname_lev_distance',
            'firstname_jw_distance', 'old_firstname_jw_distance', 
            'lastname_jw_distance', 'old_lastname_jw_distance',
            'same_first_component', 'name_in_same_row_firstname', 'name_in_same_row_old_firstname',
            'meta_in_same_row', 'all_genders_F_or_M', 'both_F_and_M_present', 
            'frequent_lastname_flag', 'max_lastname_count_id1', 'max_lastname_count_id2'
        ])

    # Precompute sets and attributes once
    attribute_dicts = {
        'max_lastname_count': sub_df.groupby('contact_uniqueid')['lastname_count'].max().to_dict(),
        'title': sub_df.groupby('contact_uniqueid')['title_standardized'].apply(set).to_dict(),
        'name': sub_df.groupby('contact_uniqueid')['entity_name'].apply(set).to_dict(),
        'entity_id': sub_df.groupby('contact_uniqueid')['entity_uniqueid'].apply(set).to_dict(),
        'system_id': sub_df.groupby('contact_uniqueid')['system_id'].apply(set).to_dict(),
        'address': sub_df.groupby('contact_uniqueid')['entity_address'].apply(set).to_dict(),
        'zip': sub_df.groupby('contact_uniqueid')['entity_zip_five'].apply(set).to_dict(),
        'state': sub_df.groupby('contact_uniqueid')['entity_state'].apply(set).to_dict(),
        'firstname': sub_df.groupby('contact_uniqueid')['firstname'].first().fillna('').to_dict(),
        'lastname': sub_df.groupby('contact_uniqueid')['lastname'].first().fillna('').to_dict(),
        'old_firstname': sub_df.groupby('contact_uniqueid')['old_firstname'].first().fillna('').to_dict(),
        'old_lastname': sub_df.groupby('contact_uniqueid')['old_lastname'].first().fillna('').to_dict(),
        'first_meta': sub_df.groupby('contact_uniqueid')['first_meta'].apply(set).to_dict(),
        'first_component': sub_df.groupby('contact_uniqueid')['first_component'].first().to_dict(),
    }

    # Use parallel processing to compute shared attributes for all pairs
    results = Parallel(n_jobs=-1)(delayed(compute_shared_attributes)(
        id1, id2, attribute_dicts, name_pairs_set, meta_pairs_set, sub_df
    ) for id1, id2 in combinations(contact_ids, 2))

    return pd.DataFrame(results)

# UPDATE GRAPHS IF ALL PAIRS ARE ACCOUNTED FOR
def build_id_map(id_set):
    """Build a mapping of IDs to the tuples they appear in."""
    id_map = defaultdict(set)
    for id1, id2 in id_set:
        id_map[id1].add((id1, id2))
        id_map[id2].add((id1, id2))
    return id_map

def find_tuples_with_id_optimized(id_to_match, id_map):
    """Efficiently find tuples using a precomputed map."""
    return id_map.get(id_to_match, set())

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

# HELPER 
def cast_types(df):
    column_type_mapping = {'last_meta': 'object',
 'first_component': 'float64',
 'contact_id1': 'object',
 'contact_id2': 'object',
 'shared_titles': 'object',
 'shared_names': 'object',
 'shared_entity_ids': 'object',
 'shared_system_ids': 'object',
 'shared_addresses': 'object',
 'shared_zips': 'object',
 'shared_states': 'object',
 'distinct_state_count': 'int64',
 'firstname_lev_distance': 'int64',
 'old_firstname_lev_distance': 'int64',
 'lastname_lev_distance': 'int64',
 'old_lastname_lev_distance': 'int64',
 'firstname_jw_distance': 'float64',
 'old_firstname_jw_distance': 'float64',
 'lastname_jw_distance': 'float64',
 'old_lastname_jw_distance': 'float64',
 'same_first_component': 'int64',
 'name_in_same_row_firstname': 'bool',
 'name_in_same_row_old_firstname': 'bool',
 'meta_in_same_row': 'bool',
 'all_genders_F_or_M': 'bool',
 'both_F_and_M_present': 'bool',
 'frequent_lastname_flag': 'bool',
 'max_lastname_count_id1': 'int64',
 'max_lastname_count_id2': 'int64',
 'shared_titles_flag': 'int64',
 'shared_names_flag': 'int64',
 'shared_entity_ids_flag': 'int64',
 'shared_system_ids_flag': 'int64',
 'shared_addresses_flag': 'int64',
 'shared_zips_flag': 'int64',
 'total_shared_attributes': 'int64'}

    for col in df.columns:
        expected_type = column_type_mapping[col]
        actual_type = df[col].dtype
        if actual_type == 'object' and expected_type != 'object':
            df[col] = pd.to_numeric(df[col], 
                                                errors='coerce')
                
            if expected_type == 'float64':
                df[col] = df[col].astype(float)

    return df