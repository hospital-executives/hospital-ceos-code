from joblib import Parallel, delayed
import pandas as pd
import Levenshtein
import jellyfish
from itertools import combinations

# crying


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
def compute_shared_attributes(id1, id2, attribute_dicts, name_pairs_set, meta_pairs_set, sub_df):
    # Compute shared attributes
    shared_titles = attribute_dicts['title'][id1] & attribute_dicts['title'][id2]
    shared_names = attribute_dicts['name'][id1] & attribute_dicts['name'][id2]
    shared_entity_ids = attribute_dicts['entity_id'][id1] & attribute_dicts['entity_id'][id2]
    shared_system_ids = attribute_dicts['system_id'][id1] & attribute_dicts['system_id'][id2]
    shared_addresses = attribute_dicts['address'][id1] & attribute_dicts['address'][id2]
    shared_zips = attribute_dicts['zip'][id1] & attribute_dicts['zip'][id2]
    shared_states = attribute_dicts['state'][id1] & attribute_dicts['state'][id2]
    distinct_state_count = len(attribute_dicts['state'][id1] | attribute_dicts['state'][id2])

    # Compute distances and similarities for firstname and old_firstname
    firstname_lev_distance = Levenshtein.distance(attribute_dicts['firstname'][id1], attribute_dicts['firstname'][id2])
    old_firstname_lev_distance = Levenshtein.distance(attribute_dicts['old_firstname'][id1], attribute_dicts['old_firstname'][id2])
    firstname_jw_distance = jellyfish.jaro_winkler_similarity(attribute_dicts['firstname'][id1], attribute_dicts['firstname'][id2])
    old_firstname_jw_distance = jellyfish.jaro_winkler_similarity(attribute_dicts['old_firstname'][id1], attribute_dicts['old_firstname'][id2])

    # Compute distances and similarities for lastname and old_lastname
    lastname_lev_distance = Levenshtein.distance(attribute_dicts['lastname'][id1], attribute_dicts['lastname'][id2])
    old_lastname_lev_distance = Levenshtein.distance(attribute_dicts['old_lastname'][id1], attribute_dicts['old_lastname'][id2])
    lastname_jw_distance = jellyfish.jaro_winkler_similarity(attribute_dicts['lastname'][id1], attribute_dicts['lastname'][id2])
    old_lastname_jw_distance = jellyfish.jaro_winkler_similarity(attribute_dicts['old_lastname'][id1], attribute_dicts['old_lastname'][id2])

    same_first_component = int(attribute_dicts['first_component'][id1] == attribute_dicts['first_component'][id2])

    # Check if names and meta codes are in the same row
    name_in_same_row_firstname = (attribute_dicts['firstname'][id1], attribute_dicts['firstname'][id2]) in name_pairs_set
    name_in_same_row_old_firstname = (attribute_dicts['old_firstname'][id1], attribute_dicts['old_firstname'][id2]) in name_pairs_set

    first_meta_codes1, first_meta_codes2 = attribute_dicts['first_meta'][id1], attribute_dicts['first_meta'][id2]
    meta_in_same_row = any((m1, m2) in meta_pairs_set for m1 in first_meta_codes1 for m2 in first_meta_codes2)

    gender_set = set(sub_df[sub_df['contact_uniqueid'].isin([id1, id2])]['gender'])
    all_genders_F_or_M = gender_set.issubset({'F', 'M'})
    both_F_and_M_present = 'F' in gender_set and 'M' in gender_set

    lastname_set = set(sub_df[sub_df['contact_uniqueid'].isin([id1, id2])]['lastname_count'])
    frequent_lastname_flag = any(count > 8 for count in lastname_set)

    return {
        'last_meta': sub_df['last_meta'].iloc[0],
        'first_component': sub_df['first_component'].iloc[0],
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
        'all_genders_F_or_M': all_genders_F_or_M,
        'both_F_and_M_present': both_F_and_M_present,
        'frequent_lastname_flag': frequent_lastname_flag,
        'max_lastname_count_id1': attribute_dicts['max_lastname_count'][id1],
        'max_lastname_count_id2': attribute_dicts['max_lastname_count'][id2]
    }

def find_pairwise_shared_attributes_old(sub_df, name_pairs_set, meta_pairs_set):
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

    # Precompute for gender and lastname_count to avoid redundant filtering
    gender_set_map = {cid: set(sub_df[sub_df['contact_uniqueid'] == cid]['gender']) for cid in contact_ids}
    lastname_count_map = {cid: set(sub_df[sub_df['contact_uniqueid'] == cid]['lastname_count']) for cid in contact_ids}

    results = []
    for id1, id2 in combinations(contact_ids, 2):
        # Precompute shared attributes and set intersections
        shared_titles = attribute_dicts['title'][id1] & attribute_dicts['title'][id2]
        shared_names = attribute_dicts['name'][id1] & attribute_dicts['name'][id2]
        shared_entity_ids = attribute_dicts['entity_id'][id1] & attribute_dicts['entity_id'][id2]
        shared_system_ids = attribute_dicts['system_id'][id1] & attribute_dicts['system_id'][id2]
        shared_addresses = attribute_dicts['address'][id1] & attribute_dicts['address'][id2]
        shared_zips = attribute_dicts['zip'][id1] & attribute_dicts['zip'][id2]
        shared_states = attribute_dicts['state'][id1] & attribute_dicts['state'][id2]
        distinct_state_count = len(attribute_dicts['state'][id1] | attribute_dicts['state'][id2])

        # Compute distances and similarities for firstnames and lastnames
        firstname1, firstname2 = attribute_dicts['firstname'][id1], attribute_dicts['firstname'][id2]
        old_firstname1, old_firstname2 = attribute_dicts['old_firstname'][id1], attribute_dicts['old_firstname'][id2]

        firstname_lev_distance = Levenshtein.distance(firstname1, firstname2)
        old_firstname_lev_distance = Levenshtein.distance(old_firstname1, old_firstname2)
        firstname_jw_distance = jellyfish.jaro_winkler_similarity(firstname1, firstname2)
        old_firstname_jw_distance = jellyfish.jaro_winkler_similarity(old_firstname1, old_firstname2)

        # Compute distances and similarities for lastnames
        lastname1, lastname2 = attribute_dicts['lastname'][id1], attribute_dicts['lastname'][id2]
        old_lastname1, old_lastname2 = attribute_dicts['old_lastname'][id1], attribute_dicts['old_lastname'][id2]

        lastname_lev_distance = Levenshtein.distance(lastname1, lastname2)
        old_lastname_lev_distance = Levenshtein.distance(old_lastname1, old_lastname2)
        lastname_jw_distance = jellyfish.jaro_winkler_similarity(lastname1, lastname2)
        old_lastname_jw_distance = jellyfish.jaro_winkler_similarity(old_lastname1, old_lastname2)

        same_first_component = int(attribute_dicts['first_component'][id1] == attribute_dicts['first_component'][id2])

        # Check if names and meta codes are in the same row
        name_in_same_row_firstname = (firstname1, firstname2) in name_pairs_set or (firstname2, firstname1) in name_pairs_set
        name_in_same_row_old_firstname = (old_firstname1, old_firstname2) in name_pairs_set or (old_firstname2, old_firstname1) in name_pairs_set

        first_meta_codes1, first_meta_codes2 = attribute_dicts['first_meta'][id1], attribute_dicts['first_meta'][id2]
        meta_in_same_row = any((m1, m2) in meta_pairs_set or (m2, m1) in meta_pairs_set 
                               for m1 in first_meta_codes1 for m2 in first_meta_codes2)

        gender_set = gender_set_map[id1] | gender_set_map[id2]
        all_genders_F_or_M = gender_set.issubset({'F', 'M'})
        both_F_and_M_present = 'F' in gender_set and 'M' in gender_set

        lastname_set = lastname_count_map[id1] | lastname_count_map[id2]
        frequent_lastname_flag = any(count > 8 for count in lastname_set)

        results.append({
            'last_meta': sub_df['last_meta'].iloc[0],
            'first_component': sub_df['first_component'].iloc[0],
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
            'all_genders_F_or_M': all_genders_F_or_M,
            'both_F_and_M_present': both_F_and_M_present,
            'frequent_lastname_flag': frequent_lastname_flag,
            'max_lastname_count_id1': attribute_dicts['max_lastname_count'][id1],
            'max_lastname_count_id2': attribute_dicts['max_lastname_count'][id2]
        })

    return pd.DataFrame(results)
