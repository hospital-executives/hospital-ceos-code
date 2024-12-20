## This is the final code to check and clean the fuzzy matching
# Automate package installation
import sys
import json
import networkx as nx
import pandas as pd
from joblib import Parallel, delayed
from itertools import chain
import subprocess

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
else: 
    confirmed_path = os.path.join(user_path, "derived/auxiliary/confirmed_2.csv")
    remaining_path =  os.path.join(user_path, "derived/auxiliary/remaining_2.csv")
    final_cleaned_path =  os.path.join(user_path, "derived/py_confirmed.csv")
    final_remaining_path =  os.path.join(user_path, "derived/py_remaining.csv")
    components_path =  os.path.join(user_path, "derived/py_graph_components.json")
    code_path = os.getcwd()

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from helper_scripts import cleaned_confirmed_helper as cc
from helper_scripts import blocking_helper
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

# LAST META FIRST COMPONENT BLOCKS
grouped = input_df.groupby(['last_meta', 'first_component'])
block_results = Parallel(n_jobs=-1)(
    delayed(fuzz.parallel_blocks)(group) for _, group in grouped
)
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

## LAST META, FIRST COMPONENT ROUND 1
new_grouped = filtered_df.groupby(['last_meta', 'first_component'])

list_of_groups = list(new_grouped)
for i in range(2):
    results_list = Parallel(n_jobs=-1)(
    delayed(fuzz.find_pairwise_shared_attributes)(
        sub_df, name_pairs_set, meta_pairs_set) 
    for _, sub_df in list_of_groups[:100]
)
    
results_list = Parallel(n_jobs=-1)(
    delayed(fuzz.find_pairwise_shared_attributes)(
        sub_df, name_pairs_set, meta_pairs_set) 
    for _, sub_df in list_of_groups
)
component_pairs = pd.concat(results_list, ignore_index=True, sort=False)
component_pairs = cc.update_results(component_pairs)
component_pairs = fuzz.cast_types(component_pairs)
    
contact_dict, comp_contact_count_dict = cc.generate_pair_dicts(component_pairs)

confirmed_graph, cleaned_remaining1, comp_dropped = cc.clean_results_pt1(
        component_pairs, cleaned_ids)

cc.update_confirmed_from_dropped(confirmed_graph, comp_dropped,
                                                    comp_contact_count_dict)

comp_dropped2, comp_remaining2 = cc.clean_results_pt2(
    cleaned_remaining1, confirmed_graph, comp_dropped, new_himss)

cc.update_confirmed_from_dropped(confirmed_graph, comp_dropped2,
                                              comp_contact_count_dict)

comp_dropped3, cleaned_remaining3 = cc.clean_results_pt3(
        comp_remaining2, comp_dropped2, user_path, new_himss) 

cc.update_confirmed_from_dropped(confirmed_graph, comp_dropped,
                                                    comp_contact_count_dict)

confirmed_graph, cleaned_remaining4 = cc.clean_results_pt4(confirmed_graph, 
cleaned_remaining3,
new_himss)

cleaned_remaining_ids = set(cleaned_remaining['contact_uniqueid'])

comp_remaining, comp_dropped = cc.clean_results_pt6(cleaned_remaining4, 
                                                    comp_dropped3,
                                                    confirmed_graph,
                                                    comp_contact_count_dict,
                                                    cleaned_remaining_ids)

remaining_ids = pd.concat([comp_remaining['contact_id1'],
                                comp_remaining['contact_id2']])
cleaned_df = input_df[~input_df['contact_uniqueid'].isin(remaining_ids)]
# location 1 - 1350689 cleaned
# 402463 uncleaned

### LAST META, FIRST META ROUND 1
new_grouped = input_df.groupby(['last_meta', 'first_meta'])
block_results2 = new_grouped.apply(lambda 
                                   sub_df: cc.find_common_blocks(sub_df, 
                                                                 first_meta=True)
                                                                 ).reset_index()

cleaned2 = block_results2[(block_results2['contact_uniqueid'].apply(len) == 1)]
remaining2 = block_results2[(block_results2['contact_uniqueid'].apply(len) > 1)]

cleaned_ids2 = set(chain.from_iterable(cleaned2['contact_uniqueid']))
remaining_ids2 = set(chain.from_iterable(remaining2['contact_uniqueid'])) # 110754
unique_contact_ids2 = cleaned_ids2 - remaining_ids2

filtered_df = input_df[input_df['contact_uniqueid'].isin(remaining_ids2)]

new_grouped = filtered_df.groupby(['last_meta', 'first_meta'])
list_of_groups = list(new_grouped)
results_list = Parallel(n_jobs=-1)(
    delayed(fuzz.find_pairwise_shared_attributes)(
        sub_df, name_pairs_set, meta_pairs_set) 
    for _, sub_df in list_of_groups[:100]
)
results_list = Parallel(n_jobs=-1)(
    delayed(fuzz.find_pairwise_shared_attributes)(
        sub_df, name_pairs_set, meta_pairs_set) 
    for _, sub_df in list_of_groups
)
meta_pairs = pd.concat(results_list, ignore_index=True)

meta_pairs['contact_id_min'] = meta_pairs[['contact_id1', 'contact_id2']].min(axis=1)
meta_pairs['contact_id_max'] = meta_pairs[['contact_id1', 'contact_id2']].max(axis=1)

component_pairs['contact_id_min'] = component_pairs[['contact_id1', 'contact_id2']].min(axis=1)
component_pairs['contact_id_max'] = component_pairs[['contact_id1', 'contact_id2']].max(axis=1)

# Perform the merge on the sorted contact_id columns
merged_df = pd.merge(meta_pairs, component_pairs, 
                     on=['last_meta', 'contact_id_min', 'contact_id_max'], how='left', 
                     indicator=True)

all_meta_pairs = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])
all_meta_pairs = all_meta_pairs.loc[:, ~all_meta_pairs.columns.str.endswith('_y')]
all_meta_pairs.columns = all_meta_pairs.columns.str.replace('_x$', '', regex=True)
all_meta_pairs = all_meta_pairs.drop(columns=['contact_id_min', 'contact_id_max'])

all_meta_pairs = fuzz.cast_types(all_meta_pairs)

all_meta_pairs['contact_id1'] = all_meta_pairs['contact_id1'].astype(str)
all_meta_pairs['contact_id2'] = all_meta_pairs['contact_id2'].astype(str)

component_ids_clean = set(confirmed_graph.nodes())
meta_pairs_1 = all_meta_pairs[
    (all_meta_pairs['contact_id1'].isin(component_ids_clean)) & 
    (all_meta_pairs['contact_id2'].isin(component_ids_clean))]

meta_pairs_1 = cc.update_results(meta_pairs_1)
contact_dict, meta_1_contact_count_dict = cc.generate_pair_dicts(meta_pairs_1)

## need to determine which cases are clear 
semi_confirmed_ids = set(cleaned_df['contact_uniqueid'])

confirmed_ids = set()
remaining_ids_in_pairs = set(meta_pairs_1['contact_id1']).union(set(meta_pairs_1['contact_id2']))
for id_ in semi_confirmed_ids:
    if id_ not in remaining_ids_in_pairs:
        confirmed_ids.add(id_)

new_confirmed_ids = confirmed_ids.union(unique_contact_ids2)

meta_graph, cleaned_remaining1,cleaned_dropped1 = cc.clean_results_pt1(meta_pairs_1,
                                                                       new_confirmed_ids)

cc.update_confirmed_from_dropped(meta_graph, cleaned_dropped1,
                                                 meta_1_contact_count_dict)

cleaned_dropped2, cleaned_remaining2 = cc.clean_results_pt2(
    cleaned_remaining1, meta_graph, cleaned_dropped1, new_himss)

cc.update_confirmed_from_dropped(meta_graph, cleaned_dropped2,
                                                 meta_1_contact_count_dict)

cleaned_dropped3, cleaned_remaining3 = cc.clean_results_pt3(
        cleaned_remaining2, cleaned_dropped2, user_path, new_himss)# these are the remaining from the confirmed df 

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

cleaned_semi = input_df[input_df['contact_uniqueid'].isin(first_meta_ids)]

G1_subgraph = confirmed_graph.subgraph(common_nodes).copy()
G_combined = nx.compose(G1_subgraph, meta_graph)
connections = {node: set(G_combined.neighbors(node)) for node in G_combined.nodes()}
connections_serializable = {key: list(value) for key, value in connections.items()}
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
 
## GET TRANSITION FILES - need to update contact_uniqueid
cleaned_semi_path = os.path.join(user_path, 
                                 "derived/auxiliary/cleaned_int.csv")
cleaned_semi.to_csv(cleaned_semi_path)

same_path = os.path.join(user_path, "derived/auxiliary/same_year.csv")
diff_path = os.path.join(user_path, "derived/auxiliary/diff_year.csv")

subprocess.run(['Rscript', 'role_change_probabilities.R', cleaned_semi_path, 
                same_path, diff_path])

## CLEAN REMAINDER DFS - don't know why this took so long
cleaned_remaining_comp, dropped_comp = cc.clean_results_pt5(comp_dropped,
comp_remaining, new_himss, user_path)

cc.update_confirmed_from_dropped(confirmed_graph, dropped_comp,
                                 comp_contact_count_dict)

# part 7
comp_remaining, dropped_comp = cc.clean_results_pt7(cleaned_remaining_comp,
                                                    confirmed_graph,
                                                    dropped_comp, 
                                                    comp_contact_count_dict)

comp_confirmed_ids_final = set(confirmed_graph.nodes()) - \
set(comp_remaining['contact_id1']).union(set(comp_remaining['contact_id2']))

meta_pairs_2 = all_meta_pairs[
    (all_meta_pairs['contact_id1'].isin(comp_confirmed_ids_final)) & 
    (all_meta_pairs['contact_id2'].isin(comp_confirmed_ids_final))]

meta_pairs_2 = cc.update_results(meta_pairs_2)

contact_dict, meta_2_contact_count_dict = cc.generate_pair_dicts(meta_pairs_2)

confirmed_ids = set()
remaining_ids_in_pairs = set(meta_pairs_2['contact_id1']
                             ).union(set(meta_pairs_2['contact_id2']))
for id_ in comp_confirmed_ids_final:
    if id_ not in remaining_ids_in_pairs:
        confirmed_ids.add(id_)

new_confirmed_ids = confirmed_ids.union(unique_contact_ids2) # from block

meta_graph, cleaned_remaining1,cleaned_dropped1 = cc.clean_results_pt1(meta_pairs_2,
                                                                       new_confirmed_ids)

cc.update_confirmed_from_dropped(meta_graph, cleaned_dropped1,
                                                 meta_2_contact_count_dict)

meta_dropped, meta_remaining2 = cc.clean_results_pt2(
 cleaned_remaining1, meta_graph, cleaned_dropped1, new_himss)

cc.update_confirmed_from_dropped(meta_graph, meta_dropped,
                                                 meta_2_contact_count_dict)

cleaned_dropped3, cleaned_remaining3 = cc.clean_results_pt3(
        meta_remaining2, meta_dropped, user_path, new_himss)# these are the remaining from the confirmed df 

cc.update_confirmed_from_dropped(meta_graph, cleaned_dropped3,
                                                 meta_2_contact_count_dict)

meta_graph, meta_remaining = cc.clean_results_pt4(meta_graph, 
cleaned_remaining3,
new_himss)

cleaned_remaining4, cleaned_dropped4 = cc.clean_results_pt5(cleaned_dropped3,
meta_remaining, new_himss, user_path)

cc.update_confirmed_from_dropped(meta_graph, cleaned_dropped4,
                                 meta_2_contact_count_dict)

meta_remaining_final, meta_dropped = cc.clean_results_pt7(cleaned_remaining4,
                                                    meta_graph,
                                                    cleaned_dropped4, 
                                                    meta_2_contact_count_dict)

meta_block_ids = set(element for sublist in cleaned2['contact_uniqueid'] for 
                     element in sublist)
meta_confirmed_ids = set()
all_comp_ids = set(component_pairs['contact_id1']
                             ).union(set(component_pairs['contact_id2']))
for id_ in meta_block_ids:
    if id_ not in all_comp_ids:
        meta_confirmed_ids.add(id_)

# FINALIZE AND EXPORT CLEANED DFS
first_component_ids = set(confirmed_graph.nodes())
first_meta_ids = set(meta_graph.nodes())
#common_nodes = first_component_ids.intersection(first_meta_ids)
final_ids = new_confirmed_ids.union(first_meta_ids).union(meta_confirmed_ids)

py_cleaned = input_df[input_df['contact_uniqueid'].isin(final_ids)]
py_remaining = input_df[~input_df['contact_uniqueid'].isin(first_meta_ids)]

meta_graph = meta_graph.copy()
for u, v in confirmed_graph.edges():
    if meta_graph.has_node(u) and meta_graph.has_node(v):
        meta_graph.add_edge(u, v)

remaining_output = new_himss[~new_himss['contact_uniqueid'].isin(final_ids)]
cleaned_output = new_himss[new_himss['contact_uniqueid'].isin(final_ids)]
connections = {node: set(meta_graph.neighbors(node)) for node in meta_graph.nodes()}
connections_serializable = {key: list(value) for key, value in connections.items()}

# write dfs to csv
remaining_output.to_csv(final_remaining_path)
cleaned_output.to_csv(final_cleaned_path)
with open(components_path, 'w') as json_file:
    json.dump(connections_serializable, json_file, indent=4)
