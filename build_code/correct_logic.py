
import os
import json
import networkx as nx
import pandas as pd
from networkx.readwrite import json_graph
import csv

user_path = "/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data"

confirmed_path = os.path.join(user_path, "derived/auxiliary/confirmed_2.csv")
remaining_path =  os.path.join(user_path, "derived/auxiliary/remaining_2.csv")
final_cleaned_path =  os.path.join(user_path, "derived/py_confirmed.csv")
final_remaining_path =  os.path.join(user_path, "derived/py_remaining.csv")
components_path =  os.path.join(user_path, "derived/py_graph_components.json")
code_path = os.getcwd()


    # write
json_file =  os.path.join(user_path, "derived/auxiliary/graph_all.json")
remaining_file =  os.path.join(user_path, 
    "derived/auxiliary/remaining_all.json")
pair_file = os.path.join(user_path, 
    "derived/auxiliary/pair_results.feather")
dropped_file = os.path.join(user_path, 
    "derived/auxiliary/dropped.json")

with open(json_file, 'r') as f:
        graph_data = json.load(f)
        G_loaded = json_graph.node_link_graph(graph_data)
    
with open(dropped_file, 'r', newline='') as file:
    reader = csv.reader(file)
    loaded_dropped = list(reader)

loaded_cleaned_remaining3 = pd.read_csv(remaining_file)
loaded_pair_results = pd.read_feather(pair_file)

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

name_pairs_set, meta_pairs_set = cc.gen_name_meta_pairs(user_path)

contact_dict, contact_count_dict = cc.generate_pair_dicts(loaded_pair_results)

confirmed_graph, cleaned_remaining1, cleaned_dropped1 = cc.clean_results_pt1(
        loaded_pair_results) # remaining = 418261

cc.update_confirmed_from_dropped(confirmed_graph, cleaned_dropped1,
                                                    contact_count_dict)


cleaned_dropped3, cleaned_remaining3 = cc.clean_results_pt3(
        cleaned_remaining1, cleaned_dropped1, user_path, new_himss) # these are the remaining from the confirmed df 

# remaining = 250108

cc.update_confirmed_from_dropped(confirmed_graph, cleaned_dropped3,
                                                    contact_count_dict)

confirmed_graph, cleaned_remaining4 = cc.clean_results_pt4(confirmed_graph, 
loaded_cleaned_remaining3,
new_himss) # len remaining 4 = 3310

dropped_set = set(tuple(inner_list) for inner_list in loaded_dropped)
cleaned_remaining5, new_dropped = cc.clean_results_pt5(dropped_set,
cleaned_remaining4, new_himss, user_path)

contact_dict, contact_count_dict = cc.generate_pair_dicts(loaded_pair_results)

cc.update_confirmed_from_dropped(confirmed_graph,new_dropped,contact_count_dict)

new_himss['contact_uniqueid'] = new_himss['contact_uniqueid'].astype(int)


remaining_a = cleaned_remaining5[~((cleaned_remaining5['min_same_probability'] >=.5) &
                            (cleaned_remaining5['firstname_lev_distance'] <=1) & 
                            (cleaned_remaining5['lastname_lev_distance'] <=1))]


remaining_ids = pd.concat([remaining_a['contact_id1'],
                                remaining_a['contact_id2']])
cleaned_df = input_df[~input_df['contact_uniqueid'].isin(remaining_ids)]

new_grouped = cleaned_df.groupby(['last_meta', 'first_meta'])
block_results2 = new_grouped.apply(lambda sub_df: cc.find_common_blocks(sub_df, first_meta=True)).reset_index()

cleaned2 = block_results2[(block_results2['contact_uniqueid'].apply(len) == 1)]
remaining2 = block_results2[(block_results2['contact_uniqueid'].apply(len) > 1)]

cleaned_ids2 = set(chain.from_iterable(cleaned2['contact_uniqueid']))
remaining_ids2 = set(chain.from_iterable(remaining2['contact_uniqueid'])) # 110754
unique_contact_ids2 = cleaned_ids2 - remaining_ids2

## adding :

merged_df = pd.merge(pair_results_first, loaded_pair_results, 
                     on=['last_meta', 'contact_id1', 'contact_id2'], how='left', 
                     indicator=True)
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

new_confirmed_ids = confirmed_ids.union(unique_contact_ids2)

contact_dict, contact_count_dict = cc.generate_pair_dicts(test)

test_graph, cleaned_remaining1,cleaned_dropped1 = cc.clean_results_pt1(test,
                                                                       new_confirmed_ids)

cc.update_confirmed_from_dropped(test_graph, cleaned_dropped1,
                                                 contact_count_dict)

cleaned_dropped3, cleaned_remaining3 = cc.clean_results_pt3(
        cleaned_remaining1, cleaned_dropped1, user_path, new_himss)# these are the remaining from the confirmed df 

cc.update_confirmed_from_dropped(test_graph, cleaned_dropped3,
                                                 contact_count_dict)

cleaned_remaining4, new_dropped = cc.clean_results_pt5(cleaned_dropped3,
cleaned_remaining3, new_himss, user_path)

cc.update_confirmed_from_dropped(test_graph,new_dropped,contact_count_dict)

cleaned_remaining5 = cleaned_remaining4[~(
    (cleaned_remaining4['firstname_lev_distance'] == 0) 
    & (cleaned_remaining4['lastname_lev_distance'] == 0) &
    (cleaned_remaining4['shared_system_ids']))]
cleaned_cleaned5 = cleaned_remaining4[(
    (cleaned_remaining4['firstname_lev_distance'] == 0) 
    & (cleaned_remaining4['lastname_lev_distance'] == 0) &
    (cleaned_remaining4['shared_system_ids']))]

cc.add_to_graph_from_df(test_graph, cleaned_cleaned5)

cleaned_remaining6 = cleaned_remaining5[~(
    ((cleaned_remaining5['firstname_jw_distance'] >= 0.8) |
    (cleaned_remaining5['name_in_same_row_firstname']))
    & (cleaned_remaining5['lastname_jw_distance'] >= 0.8) 
    & ((cleaned_remaining5['shared_addresses_flag']) |
    (cleaned_remaining5['shared_entity_ids_flag']) |
     (cleaned_remaining5['shared_names_flag'])))]
cleaned_cleaned6 = cleaned_remaining5[(
    ((cleaned_remaining5['firstname_jw_distance'] >= 0.8) |
    (cleaned_remaining5['name_in_same_row_firstname']))
    & (cleaned_remaining5['lastname_jw_distance'] >= 0.8) 
    & ((cleaned_remaining5['shared_addresses_flag']) |
    (cleaned_remaining5['shared_entity_ids_flag']) |
     (cleaned_remaining5['shared_names_flag'])))]

cc.add_to_graph_from_df(test_graph, cleaned_cleaned6)

first_component_ids = set(confirmed_graph.nodes())
first_meta_ids = set(test_graph.nodes())
common_nodes = first_component_ids.intersection(first_meta_ids)

cleaned_semi = input_df[input_df['contact_uniqueid'].isin(first_meta_ids)]
remaining_semi = input_df[~input_df['contact_uniqueid'].isin(first_meta_ids)]

meta_graph = test_graph.copy()
for u, v in confirmed_graph.edges():
    if meta_graph.has_node(u) and meta_graph.has_node(v):
        meta_graph.add_edge(u, v)

graph_data = json_graph.node_link_data(meta_graph)

# Write the JSON data to a file
new_confirmed_graph = os.path.join(user_path, "derived/auxiliary/new_graph.json")
with open(new_confirmed_graph, 'w') as f:
        json.dump(graph_data, f)

new_remaining_path = os.path.join(user_path, "derived/auxiliary/new_remaining.csv")
remaining_semi.to_csv(new_remaining_path)

new_cleaned_path = os.path.join(user_path, "derived/auxiliary/new_cleaned.csv")
cleaned_semi.to_csv(new_cleaned_path)
