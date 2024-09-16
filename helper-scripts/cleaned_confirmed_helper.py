# This is the helper file for clean_confirmed
import os
import pandas as pd
import networkx as nx
import numpy as np
import jellyfish
import Levenshtein 
import Levenshtein as lev
from itertools import combinations
from geopy.distance import geodesic
from metaphone import doublemetaphone
from collections import defaultdict

# HELPER FUNCTIONS TO CLEAN DATA
def generate_metaphone(df):
    df['metaphone'] = df['firstname'].apply(lambda x: doublemetaphone(x)[0] if pd.notnull(x) else None)
    name_to_metaphone = pd.Series(df['metaphone'].values, index=df['firstname']).to_dict()
    return df, name_to_metaphone

def is_number(s):
    try:
        float(s)  # Try to convert to a float
        return True
    except ValueError:
        return False

# Function to add edges from DataFrame to graph
def add_edges_from_df(df, G):
    for _, row in df.iterrows():
        # Check if the first column can be considered numeric
        if is_number(row.iloc[0]):
            source = row.iloc[1]  # Treat second column as source if the first is numeric
            targets = row.iloc[2:]  # Remaining columns as targets
        else:
            source = row.iloc[0]  # First column as source if not numeric
            targets = row.iloc[1:]  # Subsequent columns as targets

        # Add edges to the graph, ignoring NaN and empty strings
        for target in targets.dropna():
            if target != '':
                G.add_edge(source, target)
    
    return G

def gen_name_meta_pairs(user_path):
    name_path = os.path.join(user_path, "derived/auxiliary/digraphname.csv")
    name_pairs = pd.read_csv(name_path)[['source', 'target']]
    meta_path = os.path.join(user_path, "derived/auxiliary/digraphmeta.csv")
    meta_pairs = pd.read_csv(meta_path)[['source', 'target']]

    name_pairs_dict = {}
    for name1, name2 in name_pairs.itertuples(index=False):
        if name1 not in name_pairs_dict:
            name_pairs_dict[name1] = set()
        if name2 not in name_pairs_dict:
            name_pairs_dict[name2] = set()
        name_pairs_dict[name1].add(name2)
        name_pairs_dict[name2].add(name1)

    meta_pairs_dict = {}
    for meta1, meta2 in meta_pairs.itertuples(index=False):
        if meta1 not in meta_pairs_dict:
            meta_pairs_dict[meta1] = set()
        if meta2 not in meta_pairs_dict:
            meta_pairs_dict[meta2] = set()
        meta_pairs_dict[meta1].add(meta2)
        meta_pairs_dict[meta2].add(meta1)

    name_pairs_set = set(tuple(x) for x in name_pairs.to_numpy())
    meta_pairs_set = set(tuple(x) for x in meta_pairs.to_numpy())

    return name_pairs_set, meta_pairs_set

# GENERATE BLOCKS DF
def find_common_blocks(sub_df, first_meta=None):
    # Grouping by 'contact_uniqueid' and collecting sets in a single pass
    grouped = sub_df.groupby('contact_uniqueid').agg({
        'title_standardized': set,
        'entity_name': set,
        'entity_uniqueid': set,
        'system_id': set,
        'entity_address': set,
        'entity_zip': lambda x: set(z[:5] for z in x)
    })
    
    # Finding common elements in the sets
    common_titles = set.intersection(*grouped['title_standardized']) if len(grouped) > 1 else set()
    common_names = set.intersection(*grouped['entity_name']) if len(grouped) > 1 else set()
    common_entity_ids = set.intersection(*grouped['entity_uniqueid']) if len(grouped) > 1 else set()
    common_system_ids = set.intersection(*grouped['system_id']) if len(grouped) > 1 else set()
    common_adds = set.intersection(*grouped['entity_address']) if len(grouped) > 1 else set()
    common_zips = set.intersection(*grouped['entity_zip']) if len(grouped) > 1 else set()

    # Common data to return in both cases
    common_data = {
        'contact_uniqueid': list(sub_df['contact_uniqueid'].unique()),
        'entity_state': list(sub_df['entity_state'].unique()),
        'entity_name': list(sub_df['entity_name'].unique()),
        'entity_uniqueid': list(sub_df['entity_uniqueid'].unique()),
        'entity_address': list(sub_df['entity_address'].unique()),
        'system_id': list(sub_df['system_id'].unique()),
        'entity_zip': list(sub_df['entity_zip'].str[:5].unique()),
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

# GENERATE PAIRS DF

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
        'zip': sub_df.groupby('contact_uniqueid')['entity_zip'].apply(lambda x: set(z[:5] for z in x)).to_dict(),
        'state': sub_df.groupby('contact_uniqueid')['entity_state'].apply(set).to_dict(),
        'firstname': sub_df.groupby('contact_uniqueid')['firstname'].first().fillna('').to_dict(),
        'lastname': sub_df.groupby('contact_uniqueid')['lastname'].first().fillna('').to_dict(),
        'old_firstname': sub_df.groupby('contact_uniqueid')['old_firstname'].first().fillna('').to_dict(),
        'old_lastname': sub_df.groupby('contact_uniqueid')['old_lastname'].first().fillna('').to_dict(),
        'first_meta': sub_df.groupby('contact_uniqueid')['first_meta'].apply(set).to_dict(),
        'first_component': sub_df.groupby('contact_uniqueid')['first_component'].first().to_dict(),
    }

    results = []
    for id1, id2 in combinations(contact_ids, 2):
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
        firstname1, firstname2 = attribute_dicts['firstname'][id1], attribute_dicts['firstname'][id2]
        old_firstname1, old_firstname2 = attribute_dicts['old_firstname'][id1], attribute_dicts['old_firstname'][id2]

        firstname_lev_distance = Levenshtein.distance(firstname1, firstname2)
        old_firstname_lev_distance = Levenshtein.distance(old_firstname1, old_firstname2)
        firstname_jw_distance = jellyfish.jaro_winkler_similarity(firstname1, firstname2)
        old_firstname_jw_distance = jellyfish.jaro_winkler_similarity(old_firstname1, old_firstname2)

        # Compute distances and similarities for lastname and old_lastname
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

        gender_set = set(sub_df[sub_df['contact_uniqueid'].isin([id1, id2])]['gender'])
        all_genders_F_or_M = gender_set.issubset({'F', 'M'})
        both_F_and_M_present = 'F' in gender_set and 'M' in gender_set

        lastname_set = set(sub_df[sub_df['contact_uniqueid'].isin([id1, id2])]['lastname_count'])
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

def update_results(results):
    results['shared_titles_flag'] = results['shared_titles'].apply(
        lambda x: 1 if len(x) > 0 else 0)
    results['shared_names_flag'] = results['shared_names'].apply(
        lambda x: 1 if len(x) > 0 else 0)
    results['shared_entity_ids_flag'] = results['shared_entity_ids'].apply(
        lambda x: 1 if len(x) > 0 else 0)
    results['shared_system_ids_flag'] = results['shared_system_ids'].apply(
        lambda x: 1 if len(x) > 0 else 0)
    results['shared_addresses_flag'] = results['shared_addresses'].apply(
        lambda x: 1 if len(x) > 0 else 0)
    results['shared_zips_flag'] = results['shared_zips'].apply(
        lambda x: 1 if len(x) > 0 else 0)

    # Sum the flags to get the total count of shared attributes
    results['total_shared_attributes'] = (
        results['shared_titles_flag'] +
        results['shared_names_flag'] +
        results['shared_entity_ids_flag'] +
        results['shared_system_ids_flag'] +
        results['shared_addresses_flag'] +
        results['shared_zips_flag']
    )

    return results

# IDENTIFY PAIRS/MISMATCHES
def clean_results_pt1(pair_results, confirmed_ids=None):

    # Initialize the graph and add confirmed nodes if provided
    G = nx.Graph()
    if confirmed_ids:
        G.add_nodes_from(confirmed_ids)

    # Define conditions used multiple times
    shared_condition = (
        (pair_results['shared_names'].apply(len) > 0) |
        (pair_results['shared_entity_ids'].apply(len) > 0) |
        (pair_results['shared_addresses'].apply(len) > 0) |
        (pair_results['shared_zips'].apply(len) > 0)
    )
    
    name_similarity_condition = (
        (pair_results['firstname_lev_distance'] < 2) |
        (pair_results['firstname_jw_distance'] > 0.8) |
        (pair_results['old_firstname_lev_distance'] < 2) |
        (pair_results['old_firstname_jw_distance'] > 0.8) |
        (pair_results['name_in_same_row_firstname'])
    )
    
    lastname_count_condition = (
        (pair_results['max_lastname_count_id1'] <= 8) &
        (pair_results['max_lastname_count_id2'] <= 8)
    )
    
    title_similarity_condition = (
        (pair_results['shared_titles_flag'] == 1) &
        name_similarity_condition
    )
    
    state_or_system_condition = (
        (pair_results['distinct_state_count'] == 1) |
        (pair_results['shared_system_ids_flag'] == 1)
    )

    lastname_similarity_condition = (
        (pair_results['lastname_lev_distance'] < 2) |
        (pair_results['lastname_jw_distance'] > 0.8) |
        (pair_results['old_lastname_lev_distance'] < 2) |
        (pair_results['old_lastname_jw_distance'] > 0.8)
    )
    
    # Filtering the DataFrame step by step
    remaining = pair_results[
        ~((shared_condition & name_similarity_condition & 
           lastname_count_condition & lastname_similarity_condition) |
          (state_or_system_condition & title_similarity_condition 
           & lastname_count_condition & lastname_similarity_condition) |
          ((pair_results['name_in_same_row_firstname']) &
           (pair_results['total_shared_attributes'] > 0) &
           state_or_system_condition & lastname_count_condition &
           lastname_similarity_condition) |
          ((pair_results['lastname_lev_distance'] == 0) &
           (pair_results['name_in_same_row_firstname']) &
           state_or_system_condition & lastname_count_condition) |
           (((pair_results['lastname_lev_distance'] == 0) |
            (pair_results['lastname_jw_distance'] > 0.925)) & 
            title_similarity_condition & 
            (pair_results['max_lastname_count_id1'] <= 2) &
            (pair_results['max_lastname_count_id2'] <= 2)))
    ]

    # Filter remaining for differences and collect dropped pairs
    dropped_conditions = [
        (remaining['total_shared_attributes'] == 0) & (remaining['distinct_state_count'] > 1),
        (remaining['lastname_lev_distance'] >= 3) & (~remaining['name_in_same_row_firstname']) & (remaining['distinct_state_count'] > 1),
        (~remaining['name_in_same_row_firstname']) & (remaining['distinct_state_count'] > 1) & (remaining['lastname_lev_distance'] >= 1) & (remaining['shared_titles_flag'] == 0),
        (~remaining['name_in_same_row_firstname']) & (~remaining['meta_in_same_row'].fillna(False)) & ((remaining['lastname_jw_distance'] < 0.5) | (remaining['lastname_lev_distance'] >= 3))
    ]

    dropped = [remaining[cond] for cond in dropped_conditions]
    remaining = remaining[~dropped_conditions[-1]]

    # Add pairs that match the cleaned criteria to the graph
    cleaned =  pair_results[
    ((shared_condition & name_similarity_condition & 
           lastname_count_condition & lastname_similarity_condition) |
          (state_or_system_condition & title_similarity_condition 
           & lastname_count_condition & lastname_similarity_condition) |
          ((pair_results['name_in_same_row_firstname']) &
           (pair_results['total_shared_attributes'] > 0) &
           state_or_system_condition & lastname_count_condition &
           lastname_similarity_condition) |
          ((pair_results['lastname_lev_distance'] == 0) &
           (pair_results['name_in_same_row_firstname']) &
           state_or_system_condition & lastname_count_condition) |
           (((pair_results['lastname_lev_distance'] == 0) |
            (pair_results['lastname_jw_distance'] > 0.925)) & 
            title_similarity_condition & 
            (pair_results['max_lastname_count_id1'] <= 2) &
            (pair_results['max_lastname_count_id2'] <= 2)))
    ]

    add_to_graph_from_df(G, cleaned)

    # Collect dropped pairs into a set
    pairs_set = set()
    for df in dropped:
        pairs_set.update(zip(df['contact_id1'], df['contact_id2']))

    return G, pairs_set, remaining

def clean_results_pt2(remaining, G, dropped_sets, new_himss):
    contact_id_counts = new_himss['contact_uniqueid'].value_counts()

    remaining_updated = check_ids_in_graph(remaining, G)
    
    # Convert contact IDs to strings (if necessary) and map counts
    remaining_updated[['contact_id1', 'contact_id2']] = remaining_updated[['contact_id1', 'contact_id2']].astype(str)
    remaining_updated['contact_id1_count'] = remaining_updated['contact_id1'].map(contact_id_counts).fillna(0).astype(int)
    remaining_updated['contact_id2_count'] = remaining_updated['contact_id2'].map(contact_id_counts).fillna(0).astype(int)

    # Conditions to drop rows
    condition1 = (remaining_updated['contact_id1_count'] <= 2) & (~remaining_updated['contact_id1_in_graph']) & \
                 (remaining_updated['contact_id2_count'] <= 2) & (~remaining_updated['contact_id2_in_graph'])
    condition2 = (remaining_updated['contact_id1_count'] <= 1) & (~remaining_updated['contact_id1_in_graph']) | \
                 (remaining_updated['contact_id2_count'] <= 1) & (~remaining_updated['contact_id2_in_graph'])
    condition3 = (remaining_updated['total_shared_attributes'] == 0) & \
                 (~remaining_updated['name_in_same_row_firstname']) & \
                 (remaining_updated['distinct_state_count'] > 1)

    # Apply conditions to filter the DataFrame
    dropped1 = remaining_updated[condition1]
    dropped2 = remaining_updated[condition2 & ~condition1]
    dropped3 = remaining_updated[condition3 & ~condition1 & ~condition2]

    # Combine conditions for remaining rows
    remaining = remaining_updated[~(condition1 | condition2 | condition3)]
    remaining['contact_id1'] = remaining['contact_id1'].astype(str)
    remaining['contact_id2'] = remaining['contact_id2'].astype(str)

    # Update dropped_sets with dropped pairs
    for df in [dropped1, dropped2, dropped3]:
        dropped_sets.update(zip(df['contact_id1'], df['contact_id2']))

    return dropped_sets, remaining

def clean_results_pt3(remaining, dropped_sets, user_path, new_himss, cleaned_confirmed):
    # CREATE STATE DF
    statewise_distances_long = generate_state_df(user_path)

    # CREATE DICTS
    new_himss['contact_uniqueid'] = new_himss['contact_uniqueid'].astype(str)
    id_years_dict = new_himss.groupby('contact_uniqueid')['year'].apply(set).to_dict()
    id_streaks_dict = {id_: longest_consecutive_streak(years) for id_, years in id_years_dict.items()}

    # Calculate longest streaks in a vectorized manner
    remaining[['longest_consecutive_either', 'longest_consecutive_union']] = remaining.apply(
        lambda row: pd.Series(calculate_longest_streaks(row['contact_id1'], 
                                                        row['contact_id2'], 
                                                        id_years_dict, 
                                                        id_streaks_dict)), 
        axis=1
    )

    # Create the contact_dict more efficiently using groupby
    contact_dict = cleaned_confirmed.groupby('contact_uniqueid').apply(
        lambda x: set(zip(x['year'], x['entity_state']))
    ).to_dict()

    # Calculate total distance and count different states using vectorized operations
    
    remaining[['total_distance', 'diff_state_years_count']] = remaining.apply(
        lambda row: pd.Series(calc_total_distance_and_state_counts(row, 
                                                                   contact_dict, 
                                                                   statewise_distances_long)),
        axis=1
    )

    # Filter remaining DataFrame and update dropped sets
    to_drop = remaining[(remaining['total_distance'] > 1000) & (remaining['diff_state_years_count'] > 1)]
    remaining2 = remaining[~((remaining['total_distance'] > 1000) & (remaining['diff_state_years_count'] > 1))]

    dropped_sets.update(zip(to_drop['contact_id1'], to_drop['contact_id2']))

    return dropped_sets, remaining2

def clean_results_pt4(remaining, new_himss):
    id_year_title_dict = new_himss.groupby('contact_uniqueid').apply(
    lambda x: list(zip(x['year'], x['title_standardized']))
    ).to_dict()

    remaining[['most_recent_title_id1', 'first_title_id2']] = remaining.apply(
    lambda row: get_titles_if_no_overlap(row['contact_id1'], row['contact_id2'], id_year_title_dict),
    axis=1,
    result_type='expand'
)
    return remaining

def update_confirmed_from_dropped(G, cleaned_dropped, contact_count_dict):
    # Count occurrences of each ID in cleaned_dropped
    id_count_dict = defaultdict(int)
    for tup in cleaned_dropped:
        for id in tup:
            id_count_dict[id] += 1
    
    # Add nodes to the graph where the id count matches the contact count
    matching_ids = [id for id, count in id_count_dict.items() if count == contact_count_dict.get(id, 0)]
    G.add_nodes_from(matching_ids)

    return G

def reconcile_ids_in_both_dfs(intersection_list, final_confirmed, name_pairs_set, cleaned_confirmed, cleaned_remainder):
    approved_ids = set()
    idk = set()

    # Iterate over IDs in the intersection list
    for id in intersection_list:
        if id not in final_confirmed:
            continue

        # Filter relevant rows for the current ID
        confirmed_comparison = cleaned_confirmed[cleaned_confirmed['contact_uniqueid'] == id]
        remaining_comparison = cleaned_remainder[cleaned_remainder['contact_uniqueid'] == id]

        # Calculate intersections and similarity metrics
        intersections = {col: intersect_columns(confirmed_comparison, remaining_comparison, col) 
                         for col in ['entity_name', 'entity_uniqueid', 'entity_address', 
                                     'title_standardized', 'entity_zip', 'entity_state', 'system_id']}
        
        max_levenshtein_firstname = max_levenshtein_distance(confirmed_comparison, remaining_comparison, 'old_firstname')
        max_jw_firstname = max_jaro_winkler_distance(confirmed_comparison, remaining_comparison, 'old_firstname')
        max_levenshtein_lastname = max_levenshtein_distance(confirmed_comparison, remaining_comparison, 'old_lastname')
        max_jw_lastname = max_jaro_winkler_distance(confirmed_comparison, remaining_comparison, 'old_lastname')

        all_states = pd.concat([confirmed_comparison['entity_state'], remaining_comparison['entity_state']]).unique()
        all_first_meta = pd.concat([confirmed_comparison['first_meta'], remaining_comparison['first_meta']]).unique()

        names1 = confirmed_comparison['firstname'].unique()
        names2 = remaining_comparison['firstname'].unique()
        name_in_same_row = any((name1, name2) in name_pairs_set or (name2, name1) in name_pairs_set 
                               for name1 in names1 for name2 in names2)

        # Combine conditions for approval
        conditions = [
            (len(intersections['entity_name']) > 0 or len(intersections['entity_uniqueid']) > 0 or
             len(intersections['entity_address']) > 0 or len(intersections['entity_zip']) > 0) and 
            ((max_levenshtein_firstname < 2) or (max_jw_firstname > 0.8)) and 
            ((max_levenshtein_lastname < 2) or (max_jw_lastname > 0.8)),

            ((len(all_states) == 1) or len(intersections['system_id']) > 0) and 
            len(intersections['title_standardized']) > 0 and 
            ((max_levenshtein_firstname < 2) or (max_jw_firstname > 0.8)) and 
            ((max_levenshtein_lastname < 2) or (max_jw_lastname > 0.8)),

            name_in_same_row and 
            (len(intersections['entity_name']) > 0 or len(intersections['entity_uniqueid']) > 0 or
             len(intersections['entity_address']) > 0 or len(intersections['entity_zip']) > 0) and 
            len(all_states) == 1 and len(intersections['system_id']) > 0,

            ((max_levenshtein_lastname == 0) or (max_jw_lastname > 0.975)) and 
            (name_in_same_row or len(all_first_meta) == 1) and 
            len(all_states) == 1 and len(intersections['system_id']) > 0,

            ((max_levenshtein_firstname == 0) or (max_jw_firstname > 0.975)) and 
            (len(intersections['entity_name']) > 0 or len(intersections['entity_uniqueid']) > 0 or
             len(intersections['entity_address']) > 0 or len(intersections['entity_zip']) > 0) and 
            ((len(all_states) == 1) or len(intersections['system_id']) > 0) and 
            len(intersections['title_standardized']) > 0
        ]

        if not any(conditions):
            approved_ids.add(id)
        else:
            idk.add(id)


    return approved_ids


# SUPPORT FUNCTIONS TO IDENTIFY PAIRS/MISMATCHES
# clean results pt 1
def add_to_graph_from_df(G, df, id1_col='contact_id1', id2_col='contact_id2'):
    """
    Add nodes and edges to an existing graph from a DataFrame with two columns representing connections,
    only if neither contact ID is in the remaining DataFrame.

    Parameters:
    G (nx.Graph): The existing graph to add nodes and edges to.
    df (pd.DataFrame): DataFrame containing the contact IDs.
    remaining (pd.DataFrame): DataFrame containing contact IDs to be excluded.
    id1_col (str): Name of the first contact ID column. Default is 'contact_id1'.
    id2_col (str): Name of the second contact ID column. Default is 'contact_id2'.

    Returns:
    nx.Graph: The updated graph with added nodes and edges.
    """

    # Add nodes and edges to the graph
    for _, row in df.iterrows():
        contact_id1 = row[id1_col]
        contact_id2 = row[id2_col]
        G.add_node(contact_id1)
        G.add_node(contact_id2)
        G.add_edge(contact_id1, contact_id2)

    return G

# clean results pt 2
def check_ids_in_graph(df, G, id1_col='contact_id1', id2_col='contact_id2'):

    """
    """
    # Convert the graph's nodes to a set for efficient membership checking
    graph_nodes_set = set(G.nodes())

    # Check membership and add new columns
    df[f'{id1_col}_in_graph'] = df[id1_col].apply(lambda x: x in graph_nodes_set)
    df[f'{id2_col}_in_graph'] = df[id2_col].apply(lambda x: x in graph_nodes_set)

    return df

# clean results pt 3
def generate_state_df(user_path):
    state_path = os.path.join(user_path, "supplemental/states.csv")
    statesDF = pd.read_csv(state_path)
    states = statesDF['state']  # Assuming the CSV has a column named 'State' for state names
    distance_matrix = pd.DataFrame(index=states, columns=states)

    for state1 in states:
        for state2 in states:
            if state1 == state2:
                distance_matrix.at[state1, state2] = 0
            else:
                # Get the coordinates for each state
                coord1 = (statesDF.loc[statesDF['state'] == state1, 'latitude'].values[0], 
                        statesDF.loc[statesDF['state'] == state1, 'longitude'].values[0])
                coord2 = (statesDF.loc[statesDF['state'] == state2, 'latitude'].values[0], 
                        statesDF.loc[statesDF['state'] == state2, 'longitude'].values[0])
                # Calculate the distance
                distance_matrix.at[state1, state2] = geodesic(coord1, coord2).miles

    # Convert distances to numeric type
    distance_matrix = distance_matrix.apply(pd.to_numeric)

    state_name_to_abbr = {
        'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
        'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
        'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
        'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
        'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
        'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
        'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
        'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
        'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
        'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
        'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
        'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
        'Wisconsin': 'WI', 'Wyoming': 'WY', 'District of Columbia': 'DC'
    }
    statewise_distances_df = distance_matrix.reset_index()
    statewise_distances_long = statewise_distances_df.melt(id_vars='state', var_name='state2', value_name='distance')
    statewise_distances_long['state'] = statewise_distances_long['state'].map(state_name_to_abbr)
    statewise_distances_long['state2'] = statewise_distances_long['state2'].map(state_name_to_abbr)

    return statewise_distances_long

def find_overlapping_years(contact_id1, contact_id2, contact_dict):
    years1 = {year for year, _ in contact_dict.get(contact_id1, set())}
    years2 = {year for year, _ in contact_dict.get(contact_id2, set())}
    return years1.intersection(years2)

def calc_total_distance_and_state_counts(row, contact_dict, statewise_distances_long):
        overlapping_years = find_overlapping_years(row['contact_id1'], row['contact_id2'], contact_dict)
        return calculate_total_distance_and_count_diff_states(
            row['contact_id1'], 
            row['contact_id2'], 
            overlapping_years, 
            contact_dict, 
            statewise_distances_long
        )

def calculate_total_distance_and_count_diff_states(contact_id1, contact_id2, 
                                                   overlapping_years,
                                                     contact_dict, 
                                                     statewise_distances_long):
    total_distance = 0
    diff_state_years_count = 0
    for year in overlapping_years:
        state1 = next(state for y, state in contact_dict[contact_id1] if y == year)
        state2 = next(state for y, state in contact_dict[contact_id2] if y == year)
        if state1 != state2:
            diff_state_years_count += 1
            distance_row = statewise_distances_long[
                ((statewise_distances_long['state'] == state1) & (statewise_distances_long['state2'] == state2)) |
                ((statewise_distances_long['state'] == state2) & (statewise_distances_long['state2'] == state1))
            ]
            if not distance_row.empty:
                total_distance += distance_row['distance'].values[0]
    return total_distance, diff_state_years_count

def longest_consecutive_streak(years):
    if not years:
        return 0
    sorted_years = sorted(years)
    longest_streak = 1
    current_streak = 1
    for i in range(1, len(sorted_years)):
        if sorted_years[i] == sorted_years[i - 1] + 1:
            current_streak += 1
        else:
            longest_streak = max(longest_streak, current_streak)
            current_streak = 1
    longest_streak = max(longest_streak, current_streak)
    return longest_streak

def calculate_longest_streaks(id1, id2, id_years_dict, id_streaks_dict):
    years_id1 = id_years_dict.get(id1, set())
    years_id2 = id_years_dict.get(id2, set())
    longest_streak_either = max(id_streaks_dict.get(id1, 0), id_streaks_dict.get(id2, 0))
    longest_streak_union = longest_consecutive_streak(years_id1.union(years_id2))
    return longest_streak_either, longest_streak_union

# clean results pt 4 - NEED TO MODIFY TO BREAK TIES
def get_titles_if_no_overlap(contact_id1, contact_id2, contact_dict):
    # Get the sorted lists of (year, title) tuples for each contact ID
    years_titles_1 = sorted(contact_dict.get(contact_id1, []))
    years_titles_2 = sorted(contact_dict.get(contact_id2, []))
    
    if (not years_titles_1 or not years_titles_2) or (
        set(year for year, _ in years_titles_1) & set(year for year, _ in years_titles_2)
    ):
        return np.nan, np.nan
    
    
    # Determine the most recent year-title tuple for contact_id1
    last_year_title_1 = years_titles_1[-1]
    
    # Determine the earliest year-title tuple for contact_id2
    first_year_title_2 = years_titles_2[0]
    
    # Determine which years are relevant based on the provided logic
    if last_year_title_1[0] <= first_year_title_2[0]:
        most_recent_title_first_id = last_year_title_1[1]
        first_title_second_id = first_year_title_2[1]
    else:
        most_recent_title_first_id = first_year_title_2[1]
        first_title_second_id = last_year_title_1[1]
    
    return most_recent_title_first_id, first_title_second_id

# update confirmed from dropped
def generate_pair_dicts(pair_results):
    contact_dict = defaultdict(set)

    # Iterate over each row in the DataFrame
    for _, row in pair_results.iterrows():
        contact_id1 = row['contact_id1']
        contact_id2 = row['contact_id2']
        contact_dict[contact_id1].add(contact_id2)
        contact_dict[contact_id2].add(contact_id1)

    # Convert defaultdict to a regular dictionary (optional)
    contact_dict = dict(contact_dict)
    contact_count_dict = {contact_id: len(associated_ids) for 
                        contact_id, associated_ids in contact_dict.items()}
    
    return contact_dict, contact_count_dict

# reconcile ids in both graphs
def max_levenshtein_distance(df1, df2, column):
    max_distance = 0
    for val1 in df1[column]:
        for val2 in df2[column]:
            distance = lev.distance(val1, val2)
            if distance > max_distance:
                max_distance = distance
    return max_distance

def max_jaro_winkler_distance(df1, df2, column):
    max_distance = 0
    for val1 in df1[column]:
        for val2 in df2[column]:
            distance = jellyfish.jaro_winkler_similarity(val1, val2)
            if distance > max_distance:
                max_distance = distance
    return max_distance

def intersect_columns(df1, df2, column):
    set1 = set(df1[column])
    set2 = set(df2[column])
    return set1.intersection(set2)



# LAST STEPS
def check_name_distances(matches, name_pairs_set, pairs_set = set()):
     # Extract unique pairs of contact_uniqueids, first names, and last names
    unique_pairs = matches[['contact_uniqueid_df1', 'contact_uniqueid_df2', 'firstname_df1', 'firstname_df2', 'lastname_df1', 'lastname_df2']].drop_duplicates()

    # Calculate Levenshtein and Jaro-Winkler distances using vectorized operations
    unique_pairs['lastname_levenshtein_distance'] = unique_pairs.apply(
        lambda row: Levenshtein.distance(row['lastname_df1'], row['lastname_df2']), axis=1
    )
    unique_pairs['lastname_jaro_winkler_distance'] = unique_pairs.apply(
        lambda row: jellyfish.jaro_winkler_similarity(row['lastname_df1'], row['lastname_df2']), axis=1
    )
    unique_pairs['firstname_levenshtein_distance'] = unique_pairs.apply(
        lambda row: Levenshtein.distance(row['firstname_df1'], row['firstname_df2']), axis=1
    )
    unique_pairs['firstname_jaro_winkler_distance'] = unique_pairs.apply(
        lambda row: jellyfish.jaro_winkler_similarity(row['firstname_df1'], row['firstname_df2']), axis=1
    )

    # Vectorize condition checking
    condition = (
        (unique_pairs['lastname_levenshtein_distance'] <= 2) | 
        (unique_pairs['lastname_jaro_winkler_distance'] >= 0.8)
    ) & (
        (unique_pairs['firstname_levenshtein_distance'] <= 2) | 
        (unique_pairs['firstname_jaro_winkler_distance'] >= 0.8) | 
        unique_pairs.apply(lambda row: (row['firstname_df1'], row['firstname_df2']) in name_pairs_set 
                           or (row['firstname_df2'], row['firstname_df1']) in name_pairs_set, axis=1)
    ) & (
        unique_pairs['contact_uniqueid_df1'] != unique_pairs['contact_uniqueid_df2']
    )

    # Filter based on the condition
    filtered_pairs = unique_pairs[condition]

    # Print and add to pairs_set
    for _, row in filtered_pairs.iterrows():
        print(f"Contact IDs: {row['contact_uniqueid_df1']} and {row['contact_uniqueid_df2']}")
        pairs_set.add((row['contact_uniqueid_df1'], row['contact_uniqueid_df2']))

    return pairs_set

def process_lastnames(lasthyphens_dict, final_confirmed_2, name_pairs_set, pairs_set=set()):
    # Precompute all possible double metaphones for split last names in lasthyphens_dict
    precomputed_metas = {}
    
    for lastname in lasthyphens_dict.keys():
        name_parts = lastname.split('-')
        first_part, second_part = name_parts[0], name_parts[1]
        meta1 = doublemetaphone(first_part)[0]
        meta2 = doublemetaphone(second_part)[0]
        precomputed_metas[lastname] = (meta1, meta2)
    
    # Ensure all relevant columns are treated as strings and fill NaN with empty strings
    final_confirmed_2['lastname'] = final_confirmed_2['lastname'].fillna('').astype(str)
    final_confirmed_2['firstname'] = final_confirmed_2['firstname'].fillna('').astype(str)
    final_confirmed_2['first_meta'] = final_confirmed_2['first_meta'].fillna('').astype(str)
    final_confirmed_2['first_component'] = final_confirmed_2['first_component'].fillna('').astype(str)
    final_confirmed_2['last_meta'] = final_confirmed_2['last_meta'].fillna('').astype(str)

    for lastname, tup in lasthyphens_dict.items():
        last_meta, firstname, first_meta, first_component = tup

        # Filter original_last and new_last in one step
        mask = (final_confirmed_2['lastname'] == lastname) & (
            (final_confirmed_2['firstname'] == firstname) |
            (final_confirmed_2['first_meta'] == first_meta) |
            (final_confirmed_2['first_component'] == first_component)
        )
        original_last = final_confirmed_2[mask]

        meta1, meta2 = precomputed_metas[lastname]
        new_last_mask = (
            (final_confirmed_2['last_meta'] == meta1) |
            (final_confirmed_2['last_meta'] == meta2)
        ) & (
            (final_confirmed_2['firstname'] == firstname) |
            (final_confirmed_2['first_component'] == first_component)
        )
        new_last = final_confirmed_2[new_last_mask]

        if not new_last.empty:
            merge_columns = ['title_standardized', 'entity_name', 'entity_uniqueid', 
                             'entity_address', 'entity_zip', 'entity_state', 'system_id']

            matches = find_matches(original_last, new_last, merge_columns)
            if not matches.empty:
                pairs_set = check_name_distances(matches, name_pairs_set, pairs_set)

    return pairs_set

def find_matches(df1, df2, merge_columns):
    # Initialize an empty list to store the DataFrames with matches
    match_dfs = []
    
    # Loop through each column name in the merge_columns list
    for col in merge_columns:
        # Merge the DataFrames on the current column
        match_df = pd.merge(df1, df2, on=col, suffixes=('_df1', '_df2'))
        
        # Append the resulting DataFrame to the match_dfs list
        match_dfs.append(match_df)
    
    # Combine all the DataFrames in match_dfs and drop duplicates
    all_matches = pd.concat(match_dfs).drop_duplicates()
    
    return all_matches

