## This is the final code to check and clean the fuzzy matching
# Automate package installation
import sys
import json
import networkx as nx
import pandas as pd
from joblib import Parallel, delayed
from itertools import chain
import subprocess
import os
import copy

# SET FILE PATHS
if len(sys.argv) == 13: # TAKE FILE PATHS FROM MAKEFILE
    confirmed_path = sys.argv[1]
    remaining_path = sys.argv[2]

    comp_pairs_path = sys.argv[3]
    meta_pairs_path = sys.argv[4]
    cleaned_meta_pairs_path = sys.argv[5]
    comp_single_block_path = sys.argv[6]
    meta_single_block_path = sys.argv[7]

    final_cleaned_path = sys.argv[8]
    final_remaining_path = sys.argv[9]
    components_path = sys.argv[10]
    user_path = sys.argv[11] # data dir
    code_path = sys.argv[12]

else: # FILE PATHS FOR DEBUGGING
    user_path = "/Users/katherinepapen/Library/CloudStorage/Dropbox/hospital_ceos/_data"
    confirmed_path = os.path.join(user_path, 
                                  "derived/auxiliary/confirmed_2.csv")
    remaining_path =  os.path.join(user_path, 
                                   "derived/auxiliary/remaining_2.csv")
    
    comp_pairs_path = os.path.join(user_path, 
                                  "derived/auxiliary/comp_pairs.feather")
    meta_pairs_path = os.path.join(user_path, 
                                  "derived/auxiliary/meta_pairs.feather")
    cleaned_meta_pairs_path = os.path.join(user_path, 
                                  "derived/auxiliary/cleaned_meta_pairs.feather")
    comp_single_block_path = os.path.join(user_path, 
                                  "derived/auxiliary/comp_cleaned.json")
    meta_single_block_path = os.path.join(user_path, 
                                  "derived/auxiliary/meta_cleaned.json")

    final_cleaned_path =  os.path.join(user_path, 
                                       "derived/auxiliary/py_confirmed.csv")
    final_remaining_path =  os.path.join(user_path, 
                                         "derived/auxiliary/py_remaining.csv")
    components_path =  os.path.join(user_path, 
                                    "derived/auxiliary/py_graph_components.json")
    code_path = os.getcwd()

# LOAD HELPER SCRIPTS
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from helper_scripts import cleaned_confirmed_helper as cc
from helper_scripts import fuzzy_helper as fuzz

# LOAD DATA FRAMES
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

# LOAD PAIRS
component_pairs = pd.read_feather(comp_pairs_path)
meta_pairs = pd.read_feather(meta_pairs_path)
all_meta_pairs = pd.read_feather(cleaned_meta_pairs_path)

with open(comp_single_block_path, "r") as f:
    comp_cleaned_ids = set(json.load(f))
with open(meta_single_block_path, "r") as f:
    meta_cleaned_ids = set(json.load(f))
    
comp_remaining_ids = set(input_df['contact_uniqueid']) - comp_cleaned_ids
# prepare for pairwise comparisons
filtered_df = input_df[input_df['contact_uniqueid'].isin(comp_remaining_ids)]
name_pairs_set, meta_pairs_set = cc.gen_name_meta_pairs(user_path)

component_pairs = cc.update_results(component_pairs)
component_pairs = cc.add_pair_c_suite_flag(component_pairs, input_df)

contact_dict, comp_contact_count_dict = cc.generate_pair_dicts(component_pairs)

# determine matches and mismatches by id
confirmed_graph, cleaned_remaining1, comp_dropped = cc.clean_results_pt1(
        component_pairs, comp_cleaned_ids)

cc.update_confirmed_from_dropped(confirmed_graph, comp_dropped,
                                                    comp_contact_count_dict)

comp_remaining2, comp_dropped2 = cc.clean_results_pt2(
    cleaned_remaining1, confirmed_graph, comp_dropped, new_himss)

cc.update_confirmed_from_dropped(confirmed_graph, comp_dropped2,
                                              comp_contact_count_dict)

comp_remainingb, comp_droppedb= cc.clean_results_pt10(
    confirmed_graph, comp_remaining2, comp_dropped2, new_himss)

cc.update_confirmed_from_dropped(confirmed_graph, comp_droppedb,
                                              comp_contact_count_dict)

comp_dropped3, cleaned_remaining3 = cc.clean_results_pt3(
        comp_remainingb, comp_droppedb, user_path, new_himss) 

cc.update_confirmed_from_dropped(confirmed_graph, comp_dropped,
                                                    comp_contact_count_dict)

graph_backup = copy.deepcopy(confirmed_graph)

confirmed_graph, cleaned_remaining4 = cc.clean_results_pt4(confirmed_graph, 
cleaned_remaining3,
new_himss)

cleaned_remaining_ids = set(cleaned_remaining['contact_uniqueid'].astype(str))

comp_remaining, comp_dropped = cc.clean_results_pt6(cleaned_remaining4, 
                                                    comp_dropped3,
                                                    confirmed_graph,
                                                    comp_contact_count_dict,
                                                    cleaned_remaining_ids)

comp_set = set(zip(component_pairs["contact_id1"], 
                   component_pairs["contact_id2"]))
result = fuzz.find_valid_tuples_optimized(set(comp_remaining['contact_id1']
                                         ).union(set(comp_remaining['contact_id2'])), 
                                         comp_set, comp_dropped, set(), set())

all_ids = set(input_df['contact_uniqueid'])
filtered_data = {key: value for key, value in result.items() if key in all_ids}
comp_empty_values_1 = {key: value for key, value in filtered_data.items() if 
                len(value) == 0}
confirmed_graph.add_nodes_from(comp_empty_values_1.keys())

remaining_ids = (set(comp_remaining['contact_id1']).union( 
                        set(comp_remaining['contact_id2']))) - \
                            comp_empty_values_1.keys()
cleaned_df = input_df[~input_df['contact_uniqueid'].isin(remaining_ids)]
# original - 1272262

### LAST META, FIRST META ROUND 1

unique_contact_ids2 = meta_cleaned_ids
meta_remaining_ids = set(input_df['contact_uniqueid']) - meta_cleaned_ids

filtered_df = input_df[input_df['contact_uniqueid'].isin(meta_remaining_ids)]

all_meta_pairs['contact_id1'] = all_meta_pairs['contact_id1'].astype(str)
all_meta_pairs['contact_id2'] = all_meta_pairs['contact_id2'].astype(str)

component_ids_clean = set(confirmed_graph.nodes())
meta_pairs_1 = all_meta_pairs[
    (all_meta_pairs['contact_id1'].isin(component_ids_clean)) & 
    (all_meta_pairs['contact_id2'].isin(component_ids_clean))]

meta_pairs_1 = cc.update_results(meta_pairs_1)
meta_pairs_1 = cc.add_pair_c_suite_flag(meta_pairs_1, input_df)

contact_dict, meta_1_contact_count_dict = cc.generate_pair_dicts(meta_pairs_1)

## need to determine which cases are clear 
semi_confirmed_ids = set(cleaned_df['contact_uniqueid'])

confirmed_ids = set()
remaining_ids_in_pairs = set(meta_pairs_1['contact_id1']).union(set(
    meta_pairs_1['contact_id2']))
for id_ in semi_confirmed_ids:
    if id_ not in remaining_ids_in_pairs:
        confirmed_ids.add(id_)

new_confirmed_ids = confirmed_ids.union(unique_contact_ids2)

meta_graph, cleaned_remaining1, cleaned_dropped1 = \
    cc.clean_results_pt1(meta_pairs_1, new_confirmed_ids)

cc.update_confirmed_from_dropped(meta_graph, cleaned_dropped1,
                                                 meta_1_contact_count_dict)

cleaned_remaining2, cleaned_dropped2 = cc.clean_results_pt2(
    cleaned_remaining1, meta_graph, cleaned_dropped1, new_himss)

cc.update_confirmed_from_dropped(meta_graph, cleaned_dropped2,
                                                 meta_1_contact_count_dict)

meta_remainingb, meta_droppedb= cc.clean_results_pt10(
    meta_graph, cleaned_remaining2, cleaned_dropped2, new_himss)

cc.update_confirmed_from_dropped(meta_graph, meta_droppedb,
                                              meta_1_contact_count_dict)                                                

cleaned_dropped3, cleaned_remaining3 = cc.clean_results_pt3(
        meta_remainingb, meta_droppedb, user_path, new_himss)# these are the remaining from the confirmed df 

cc.update_confirmed_from_dropped(meta_graph, cleaned_dropped3,
                                                 meta_1_contact_count_dict)

meta_graph, cleaned_remaining4 = cc.clean_results_pt4(meta_graph, 
cleaned_remaining3,
new_himss)

meta_remaining, meta_dropped = cc.clean_results_pt6(cleaned_remaining4,
                                                    cleaned_dropped3,
                                                    meta_graph,
                                                    meta_1_contact_count_dict,
                                                    cleaned_remaining_ids)

first_component_ids = set(confirmed_graph.nodes())
first_meta_ids = set(meta_graph.nodes())
common_nodes = first_component_ids.intersection(first_meta_ids)

# update IDs for transition files
cleaned_semi = input_df[input_df['contact_uniqueid'].isin(first_meta_ids)]

G1_subgraph = copy.deepcopy(confirmed_graph.subgraph(common_nodes))
G_combined = nx.compose(G1_subgraph, meta_graph)
connections = {node: set(G_combined.neighbors(node)) for node in
              G_combined.nodes()}
connections_serializable = {key: list(value) for key, value in 
                            connections.items()}
converted_dict = {key: [int(item) for item in value] 
                  for key, value in connections_serializable.items() 
                  if len(value) > 0}
precomputed_max = {
    key: key if int(key) > max(value_list) else max(value_list)
    for key, value_list in converted_dict.items()
}
def map_to_max(id_value):
    return precomputed_max.get(id_value, id_value)

cleaned_semi['contact_uniqueid'] = \
    cleaned_semi['contact_uniqueid'].astype(str).map(map_to_max)


remaining_semi = input_df[~input_df['contact_uniqueid'].isin(first_meta_ids)]
 
## GET TRANSITION FILES
cleaned_semi_path = os.path.join(user_path, 
                                 "derived/auxiliary/cleaned_int.csv")
cleaned_semi.to_csv(cleaned_semi_path)

same_path = os.path.join(user_path, "derived/auxiliary/same_year.csv")
diff_path = os.path.join(user_path, "derived/auxiliary/diff_year.csv")

subprocess.run(['Rscript', 'role_change_probabilities.R', cleaned_semi_path, 
                same_path, diff_path])

## CLEAN IDS USING TRANSITION FILES
# clean remaining component pairs
remaining_backup = copy.deepcopy(comp_remaining)


cleaned_remaining_comp, dropped_comp = cc.clean_results_pt5(comp_dropped,
comp_remaining, new_himss, user_path)

cc.update_confirmed_from_dropped(confirmed_graph, dropped_comp,
                                 comp_contact_count_dict)

comp_remaining, dropped_comp = cc.clean_results_pt7(cleaned_remaining_comp,
                                                    confirmed_graph,
                                                    dropped_comp, 
                                                    comp_contact_count_dict)

cc.update_confirmed_from_dropped(confirmed_graph, dropped_comp,
                                 comp_contact_count_dict)

comp_remaining, dropped_comp = cc.clean_results_pt2(comp_remaining, 
                                                    confirmed_graph, 
                                                    dropped_comp, new_himss)

cc.update_confirmed_from_dropped(confirmed_graph, dropped_comp,
                                 comp_contact_count_dict)

comp_remaining, dropped_comp = cc.clean_results_pt8(comp_remaining,
                                                    confirmed_graph,
                                                    dropped_comp, 
                                                    comp_contact_count_dict)

cc.update_confirmed_from_dropped(confirmed_graph, dropped_comp,
                                 comp_contact_count_dict)

comp_remaining, dropped_comp = cc.clean_results_pt9(comp_remaining,
                                                    confirmed_graph,
                                                    dropped_comp, 
                                                    comp_contact_count_dict,
                                                    new_himss)

cc.update_confirmed_from_dropped(confirmed_graph, dropped_comp,
                                 comp_contact_count_dict)


# can remove once done debugging
import copy
confirmed_graph_backup = copy.deepcopy(confirmed_graph)

# update if ids are all accounted for - go back and check this might not
# be necessary here
comp_set = set(zip(component_pairs["contact_id1"], 
                   component_pairs["contact_id2"]))

result = fuzz.find_valid_tuples_optimized(set(comp_remaining['contact_id1']
                                         ).union(set(comp_remaining['contact_id2'])), 
                                         comp_set, dropped_comp, set(), set())

all_ids = set(input_df['contact_uniqueid'])
filtered_data = {key: value for key, value in result.items() if key in all_ids}
comp_empty_values2 = {key: value for key, value in filtered_data.items() if 
                len(value) == 0}
#confirmed_graph.add_nodes_from(set(comp_empty_values2.keys()))

comp_confirmed_ids_final = (set(confirmed_graph.nodes()) - \
set(comp_remaining['contact_id1']).union(set(comp_remaining['contact_id2']))
).union(set(comp_empty_values2.keys() - outlier_ids))

true_comp_remaining  = set(comp_remaining['contact_id1']).union(
    set(comp_remaining['contact_id2'])) - \
        (set(comp_empty_values2.keys()) - outlier_ids) - \
        (set(comp_empty_values_1.keys() - outlier_ids))
#comp_missing = set( str(x) for x in missing_contact) & true_comp_remaining

# determine new meta pairs and clean
meta_pairs_2 = all_meta_pairs[
    (all_meta_pairs['contact_id1'].isin(comp_confirmed_ids_final)) & 
    (all_meta_pairs['contact_id2'].isin(comp_confirmed_ids_final))]

meta_pairs_2 = cc.update_results(meta_pairs_2)
meta_pairs_2 = cc.add_pair_c_suite_flag(meta_pairs_2, input_df)

contact_dict, meta_2_contact_count_dict = cc.generate_pair_dicts(meta_pairs_2)

confirmed_ids = set()
remaining_ids_in_pairs = set(meta_pairs_2['contact_id1']
                             ).union(set(meta_pairs_2['contact_id2']))
for id_ in comp_confirmed_ids_final:
    if id_ not in remaining_ids_in_pairs:
        confirmed_ids.add(id_) # ids in comp that are not in meta

# we only want to add ids that are unique to their meta block 
# (unique_contact_ids2) if: 
# a) they do not appear at all in the comp pairs or
# b) they are clean in the comp_graph
meta_confirmed_ids = set()
ids_to_check = set(comp_remaining['contact_id1']
                             ).union(set(comp_remaining['contact_id2'])) - \
                             set(comp_empty_values2.keys())
for id_ in unique_contact_ids2:
    if id_ not in ids_to_check:
        meta_confirmed_ids.add(id_) 

new_confirmed_ids = confirmed_ids.union(meta_confirmed_ids) # from meta block

meta_graph, cleaned_remaining1,cleaned_dropped1 = \
    cc.clean_results_pt1(meta_pairs_2, new_confirmed_ids)

cc.update_confirmed_from_dropped(meta_graph, cleaned_dropped1,
                                                 meta_2_contact_count_dict)

meta_remaining2, meta_dropped = cc.clean_results_pt2(
cleaned_remaining1, meta_graph, cleaned_dropped1, new_himss)

cc.update_confirmed_from_dropped(meta_graph, meta_dropped,
                                                 meta_2_contact_count_dict)

meta_remainingb, meta_droppedb = cc.clean_results_pt10(
    meta_graph, meta_remaining2, meta_dropped, new_himss)

cc.update_confirmed_from_dropped(meta_graph, meta_droppedb,
                                              meta_2_contact_count_dict)

cleaned_dropped3, cleaned_remaining3 = cc.clean_results_pt3(
        meta_remainingb, meta_droppedb, user_path, new_himss)# these are the remaining from the confirmed df 

cc.update_confirmed_from_dropped(meta_graph, cleaned_dropped3,
                                                 meta_2_contact_count_dict)

meta_graph, meta_remaining = cc.clean_results_pt4(meta_graph, 
cleaned_remaining3,
new_himss)

cleaned_remaining4, cleaned_dropped4 = cc.clean_results_pt5(cleaned_dropped3,
meta_remaining, new_himss, user_path)

cc.update_confirmed_from_dropped(meta_graph, cleaned_dropped4,
                                 meta_2_contact_count_dict)

cleaned_remaining5, cleaned_dropped5 = cc.clean_results_pt7(cleaned_remaining4,
                                                    meta_graph,
                                                    cleaned_dropped4, 
                                                    meta_2_contact_count_dict)

meta_remaining, meta_dropped = cc.clean_results_pt8(cleaned_remaining5,meta_graph,
                                          cleaned_dropped5, 
                                         meta_2_contact_count_dict)

cc.update_confirmed_from_dropped(meta_graph, meta_dropped,
                                 meta_2_contact_count_dict)

meta_remaining, meta_dropped = cc.clean_results_pt9(meta_remaining,
                                                    meta_graph,
                                                    meta_dropped, 
                                                    meta_2_contact_count_dict,
                                                    new_himss)

cc.update_confirmed_from_dropped(meta_graph, meta_dropped,
                                 meta_2_contact_count_dict)


## GENERATE FINAL DATA FRAMES

# get confirmed IDs
first_component_ids = set(confirmed_graph.nodes())
first_meta_ids = set(meta_graph.nodes())

# generate final graph with all connections
final_graph = copy.deepcopy(meta_graph)

for u, v in confirmed_graph.edges():
    if final_graph.has_node(u) and final_graph.has_node(v):
        final_graph.add_edge(u, v)

# remaining_ids = set(zip(meta_remaining['contact_id1'], meta_remaining['contact_id2']))
#set(input_df['contact_uniqueid']) - final_graph.nodes()
meta_set = set(zip(meta_pairs["contact_id1"], meta_pairs["contact_id2"]))

# manually clean remaining CEOs
manual_pairs_path = os.path.join(user_path, 
                                 "derived/auxiliary/manual_pairs.json")

manual_pairs_path_2 = os.path.join(user_path, 
                                 "derived/auxiliary/manual_pairs_2.json")

def load_manual_pairs(file_path):
    with open(file_path, "r") as f:
        temp = json.load(f)
    
    return {
        "confirmed_same": [tuple(t) for t in temp["confirmed_same"]],
        "confirmed_diff": [tuple(t) for t in temp["confirmed_diff"]]
    }

# Load both files
data_1 = load_manual_pairs(manual_pairs_path)
data_2 = load_manual_pairs(manual_pairs_path_2)

# Merge the lists
combined_confirmed_same = list(set(data_1["confirmed_same"] + 
                                   data_2["confirmed_same"]))
combined_confirmed_diff = list(set(data_1["confirmed_diff"] + 
                                   data_2["confirmed_diff"]))
manual_pairs = set(combined_confirmed_same).union(combined_confirmed_diff)

# need to update graph + remaining for manual CEOs 
final_graph.add_edges_from(combined_confirmed_same)

remaining_ids = set(meta_remaining['contact_id1']).union( 
                        meta_remaining['contact_id2'])
new_comp_dropped = dropped_comp.union(set(combined_confirmed_diff)
                                      ).union(set(combined_confirmed_same))
new_meta_dropped = meta_dropped.union(set(combined_confirmed_diff)
                                      ).union(set(combined_confirmed_same))
result = fuzz.find_valid_tuples_optimized(remaining_ids, comp_set, 
                                          new_comp_dropped, meta_set, 
                                          new_meta_dropped)

empty_values = {key: value for key, value in result.items() if len(value) == 0}
cleaned_empty_values = empty_values.keys() - outlier_ids
final_graph.add_nodes_from(cleaned_empty_values)

# get final ids 
updated_comp_remaining = comp_remaining[
    ~comp_remaining.apply(lambda row: (row['contact_id1'], row['contact_id2']) 
                          in manual_pairs or 
                          (row['contact_id2'], row['contact_id1']) 
                          in manual_pairs, axis=1)]
updated_meta_remaining = meta_remaining[
    ~meta_remaining.apply(lambda row: (row['contact_id1'], row['contact_id2']) 
                          in manual_pairs or 
                          (row['contact_id2'], row['contact_id1']) 
                          in manual_pairs, axis=1)]

meta_remaining_ids = set(updated_meta_remaining["contact_id1"]).union(
                            set(updated_meta_remaining["contact_id2"])) - \
                                cleaned_empty_values
comp_remaining_ids =  set(updated_comp_remaining["contact_id1"]).union(
                            set(updated_comp_remaining["contact_id2"])) - \
                            cleaned_empty_values.union(
                                (comp_empty_values2.keys() - outlier_ids)
                            ) 
final_ids = (final_graph.nodes() - 
             meta_remaining_ids).union(cleaned_empty_values
                                       )

# need union with early cleaned values
unaccounted = comp_remaining_ids.union(meta_remaining_ids).union(outlier_ids)
new_final = (set(new_himss['contact_uniqueid']) - unaccounted).\
    union(cleaned_empty_values).union(comp_empty_values2).union(
                                           set(['1310316', '1350577', '1353537',
                                                '553093','84326', '1354846',
                                                '1354991'])
                                       )
new_py_cleaned = input_df[input_df['contact_uniqueid'].isin(new_final)]
new_py_cleaned.loc[new_py_cleaned['full_name'] == 'tonyahale', 
                   'contact_uniqueid'] = '590074'
new_py_cleaned.loc[new_py_cleaned['full_name'] == 'jamesheitzenrater', 
                   'contact_uniqueid'] = '2301043'

py_cleaned = new_py_cleaned[new_py_cleaned['contact_uniqueid'].isin(new_final)]

# create and save cleaned data frames

py_remaining = new_himss[~new_himss['id'].isin(py_cleaned['id'])]

connections = {node: set(final_graph.neighbors(node)) for node in 
               final_graph.nodes()}
connections_serializable = {key: list(value) for key, value in 
                            connections.items()}

py_cleaned.to_csv(final_cleaned_path)
py_remaining.to_csv(final_remaining_path)
with open(components_path, 'w') as json_file:
   json.dump(connections_serializable, json_file, indent=4)
