
import os
import sys 
import json
import csv
import pandas as pd
import networkx as nx
from itertools import chain
from networkx.readwrite import json_graph
from joblib import Parallel, delayed


user_path = "/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data"
# TAKE ARGS FROM MAKEFILE
# Get arguments from the command line
if len(sys.argv) == 8:
    confirmed_path = sys.argv[1]
    remaining_path = sys.argv[2]
    final_cleaned_path = sys.argv[3]
    final_remaining_path = sys.argv[4]
    components_path = sys.argv[5]
    user_path = sys.argv[6] # data dir
    code_path = sys.argv[7]
    # need to modify to add new files
    # outliers path
else: 
    confirmed_path = os.path.join(user_path, "derived/auxiliary/confirmed_2.csv")
    remaining_path =  os.path.join(user_path, "derived/auxiliary/remaining_2.csv")
    final_cleaned_path =  os.path.join(user_path, "derived/py_confirmed.csv")
    final_remaining_path =  os.path.join(user_path, "derived/py_remaining.csv")
    components_path =  os.path.join(user_path, "derived/py_graph_components.json")
    code_path = os.getcwd()
    json_file =  os.path.join(user_path, "derived/auxiliary/graph_all.json")
    remaining_file =  os.path.join(user_path, 
    "derived/auxiliary/remaining_all.json")
    pair_file = os.path.join(user_path, 
    "derived/auxiliary/pair_results.feather")
    dropped_file = os.path.join(user_path, 
    "derived/auxiliary/dropped.json")

# load helper files
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from helper_scripts import cleaned_confirmed_helper as cc
from helper_scripts import blocking_helper
from helper_scripts import fuzzy_helper as fuzz

# load dfs and set variable(s)
cleaned_confirmed = pd.read_csv(confirmed_path)
cleaned_remaining = pd.read_csv(remaining_path)
new_himss = pd.concat([cleaned_confirmed, cleaned_remaining], axis=0)
new_himss['contact_uniqueid'] = new_himss['contact_uniqueid'].astype(str)
regenerate_hyphenated_pairs = None

outliers_path = os.path.join(user_path, "derived/auxiliary/outliers.csv")
outliers = pd.read_csv(outliers_path)
outlier_ids = set(outliers['contact_uniqueid'].astype(str))

input_df = new_himss[~new_himss['contact_uniqueid'].isin(outlier_ids)]
input_df['contact_uniqueid'] = input_df['contact_uniqueid'].apply(str)

# create blocks
grouped = input_df.groupby(['last_meta', 'first_component'])
block_results = Parallel(n_jobs=-1)(
    delayed(fuzz.parallel_blocks)(group) for _, group in grouped
) #193000

block_component_pairs = pd.DataFrame(block_results)

# separate
cleaned1 = block_component_pairs[
    (block_component_pairs['contact_uniqueid'].apply(len) == 1)]
remaining1 = block_component_pairs[
    (block_component_pairs['contact_uniqueid'].apply(len) > 1)]

temp_cleaned_ids = set(chain.from_iterable(cleaned1['contact_uniqueid']))
remaining_ids = set(chain.from_iterable(remaining1['contact_uniqueid'])) # 110754
cleaned_ids = temp_cleaned_ids - remaining_ids 

# prepare for pairwise comparisons
filtered_df = input_df[input_df['contact_uniqueid'].isin(remaining_ids)]
name_pairs_set, meta_pairs_set = cc.gen_name_meta_pairs(user_path)

new_grouped = filtered_df.groupby(['last_meta', 'first_component'])
new_grouped_list = list(new_grouped)
for i in (0,1,2):
    print(i)
    results = Parallel(n_jobs=-1)(
    delayed(fuzz.find_pairwise_shared_attributes)(
        sub_df, name_pairs_set, meta_pairs_set) 
    for _, sub_df in new_grouped_list[:100]
)
    
results = Parallel(n_jobs=-1)(
    delayed(fuzz.find_pairwise_shared_attributes)(
        sub_df, name_pairs_set, meta_pairs_set) 
    for _, sub_df in new_grouped_list
)

component_pairs = pd.concat(results, ignore_index=True)
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

for col in component_pairs.columns:
    expected_type = column_type_mapping[col]
    actual_type = component_pairs[col].dtype
    if actual_type == 'object' and expected_type != 'object':
        component_pairs[col] = pd.to_numeric(component_pairs[col], errors='coerce')
            
        if expected_type == 'float64':
            component_pairs[col] = component_pairs[col].astype(float)

component_pairs = cc.update_results(component_pairs)
contact_dict, contact_count_dict = cc.generate_pair_dicts(component_pairs)

# CLEAN DF
confirmed_graph, cleaned_remaining1, dropped_ids = cc.clean_results_pt1(
        component_pairs, cleaned_ids)

cc.update_confirmed_from_dropped(confirmed_graph, dropped_ids,
                                                    contact_count_dict)

# NEED TO GO BACK AND ADD LOGIC + VERIFY LOGIC
cleaned_dropped2, cleaned_remaining2 = cc.clean_results_pt2(
    cleaned_remaining1, confirmed_graph, dropped_ids, new_himss)

cc.update_confirmed_from_dropped(confirmed_graph, cleaned_dropped2,
                                                    contact_count_dict)

cleaned_dropped3, cleaned_remaining3 = cc.clean_results_pt3(
        cleaned_remaining2, cleaned_dropped2, user_path, new_himss, 
        cleaned_confirmed)

# need to check that you are assigned distance correctly

cc.update_confirmed_from_dropped(confirmed_graph, cleaned_dropped3,
                                                    contact_count_dict)

cleaned_remaining4 = cc.clean_results_pt4(confirmed_graph, 
cleaned_remaining3, new_himss)

# need to decide if you want to save the data
# also need to decide how to integrate probabilities
# decision: get first round confirmed without probabilities; generate
# probabilities from this and then go back and clean remaining pairs

# so clean 1-4 , generate probabilities, clean 5

remaining_ids = pd.concat([cleaned_remaining4['contact_id1'],
                                cleaned_remaining4['contact_id2']])
input_df['contact_uniqueid'] = input_df['contact_uniqueid'].astype(int)
cleaned_df = input_df[~input_df['contact_uniqueid'].isin(remaining_ids)]

# conditions
# cleaned_remaining3[cleaned_remaining3['id2_last_has_one_first'] & cleaned_remaining3['id1_last_has_one_first'] & (cleaned_remaining3['lastname_lev_distance']==0)]

new_grouped = cleaned_df.groupby(['last_meta', 'first_meta'])
results = Parallel(n_jobs=-1)(
    delayed(fuzz.find_pairwise_shared_attributes)(
        sub_df, name_pairs_set, meta_pairs_set) 
    for _, sub_df in new_grouped
)
pair_results_first = pd.concat(results, ignore_index=True)
pair_results_first = cc.update_results(pair_results_first) 
merged_df = pd.merge(pair_results_first
, pair_results, on=['last_meta', 'contact_id1', 'contact_id2'], how='left', indicator=True)
test = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])
test = test.loc[:, ~test.columns.str.endswith('_y')]
test.columns = test.columns.str.replace('_x$', '', regex=True)
test = cc.update_results(test) 

## need to determine which cases are clear
final_confirmed_ids = set(cleaned_df['contact_uniqueid'])

confirmed_ids = set()
remaining_ids_in_pairs = set(test['contact_id1']).union(set(test['contact_id2']))
for id_ in final_confirmed_ids:
    if id_ not in remaining_ids_in_pairs:
        confirmed_ids.add(id_)


contact_dict, contact_count_dict = cc.generate_pair_dicts(test)

test_graph, cleaned_dropped1, cleaned_remaining1 = cc.clean_results_pt1(test)

cc.update_confirmed_from_dropped(test_graph, cleaned_dropped1,
                                                 contact_count_dict)

cleaned_dropped2, cleaned_remaining2 = cc.clean_results_pt2(
    cleaned_remaining1, test_graph, cleaned_dropped1, new_himss)

cc.update_confirmed_from_dropped(test_graph, cleaned_dropped2,
                                                 contact_count_dict)

cleaned_dropped3, cleaned_remaining3 = cc.clean_results_pt3(
    cleaned_remaining2, cleaned_dropped2, user_path, new_himss, 
    cleaned_confirmed) # these are the remaining from the confirmed df 

cc.update_confirmed_from_dropped(test_graph, cleaned_dropped3,
                                                 contact_count_dict)

cleaned_remaining4, new_dropped = cc.clean_results_pt5(cleaned_dropped3,
cleaned_remaining3, new_himss, user_path)

cc.update_confirmed_from_dropped(test_graph,new_dropped,contact_count_dict)

# generate cleaned data frame from the intersection of the two graphs
# pass cleaned data frame to RMD to get job transition probabilities
# use probabilities to clean the remainder of BOTH graphs
# add new rules 
# also add/clean last name anomalies