import os
import sys 
import json
import csv
import pandas as pd
import networkx as nx
from itertools import chain
from networkx.readwrite import json_graph

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

    # add
    regenerate_initial_blocks = True

    # write
    json_file =  os.path.join(user_path, "derived/auxiliary/graph_all.json")
    remaining_file =  os.path.join(user_path, 
    "derived/auxiliary/remaining_all.json")
    pair_file = os.path.join(user_path, 
    "derived/auxiliary/pair_results.feather")
    dropped_file = os.path.join(user_path, 
    "derived/auxiliary/dropped.json")

else: 
    confirmed_path = os.path.join(user_path, "derived/auxiliary/confirmed_2.csv")
    remaining_path =  os.path.join(user_path, "derived/auxiliary/remaining_2.csv")
    final_cleaned_path =  os.path.join(user_path, "derived/py_confirmed.csv")
    final_remaining_path =  os.path.join(user_path, "derived/py_remaining.csv")
    components_path =  os.path.join(user_path, "derived/py_graph_components.json")
    code_path = os.getcwd()

    # add
    regenerate_initial_blocks = True

    # write
    json_file =  os.path.join(user_path, "derived/auxiliary/graph_all.json")
    remaining_file =  os.path.join(user_path, 
    "derived/auxiliary/remaining_all.json")
    pair_file = os.path.join(user_path, 
    "derived/auxiliary/pair_results.feather")
    dropped_file = os.path.join(user_path, 
    "derived/auxiliary/dropped.json")


#raise ValueError

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from helper_scripts import cleaned_confirmed_helper as cc
from helper_scripts import blocking_helper

# load dfs and set variable(s)
cleaned_confirmed = pd.read_csv(confirmed_path)
cleaned_remaining = pd.read_csv(remaining_path)
new_himss = pd.concat([cleaned_confirmed, cleaned_remaining], axis=0)
regenerate_hyphenated_pairs = None

outliers_path = os.path.join(user_path, "derived/auxiliary/outliers.csv")
outliers = pd.read_csv(outliers_path)
outlier_ids = set(outliers['contact_uniqueid'])
input_df = new_himss[~new_himss['contact_uniqueid'].isin(outlier_ids)]


if regenerate_initial_blocks:
###################### LAST META AND FIRST COMPONENT ######################
    #### BLOCK LEVEL COMPARISONS ####
    print('block level')
    input_df['contact_uniqueid'] = input_df['contact_uniqueid'].apply(str)
    grouped = input_df.groupby(['last_meta', 'first_component'])
    block_results = grouped.apply(cc.find_common_blocks).reset_index() # takes 10 min

    # separate into confirmed/remaining
    cleaned1 = block_results[(block_results['contact_uniqueid'].apply(len) == 1)]
    remaining1 = block_results[(block_results['contact_uniqueid'].apply(len) > 1)]

    cleaned_ids = set(chain.from_iterable(cleaned1['contact_uniqueid']))
    remaining_ids = set(chain.from_iterable(remaining1['contact_uniqueid'])) # 110754
    unique_contact_ids = cleaned_ids - remaining_ids #83078

    # final remaining from blockwise comparisons
    filtered_df = input_df[input_df['contact_uniqueid'
                                                    ].isin(remaining_ids)]

    name_pairs_set, meta_pairs_set = cc.gen_name_meta_pairs(user_path)

    #### PAIRWISE LEVEL COMPARISONS ####
    print('pairwise level')
    new_grouped = filtered_df.groupby(['last_meta', 'first_component'])
    pair_results = pd.concat([cc.find_pairwise_shared_attributes(sub_df, 
                                                                name_pairs_set,
                                                                meta_pairs_set) 
                                                                for _, 
                        sub_df in new_grouped]) # 25 min
    pair_results = cc.update_results(pair_results) # 110754 - correct

    contact_dict, contact_count_dict = cc.generate_pair_dicts(pair_results)

    confirmed_graph, cleaned_dropped1, cleaned_remaining1 = cc.clean_results_pt1(
        pair_results, unique_contact_ids)

    cc.update_confirmed_from_dropped(confirmed_graph, cleaned_dropped1,
                                                    contact_count_dict)

    cleaned_dropped2, cleaned_remaining2 = cc.clean_results_pt2(
        cleaned_remaining1, confirmed_graph, cleaned_dropped1, new_himss)

    cc.update_confirmed_from_dropped(confirmed_graph, cleaned_dropped2,
                                                    contact_count_dict)

    cleaned_dropped3, cleaned_remaining3 = cc.clean_results_pt3(
        cleaned_remaining2, cleaned_dropped2, user_path, new_himss, 
        cleaned_confirmed) # these are the remaining from the confirmed df 

    cc.update_confirmed_from_dropped(confirmed_graph, cleaned_dropped3,
                                                    contact_count_dict)

    graph_data = json_graph.node_link_data(confirmed_graph)

    # Write the JSON data to a file
    with open(json_file, 'w') as f:
        json.dump(graph_data, f)

    cleaned_remaining3.to_csv(remaining_file)
    pair_results['first_component'] = pd.to_numeric(pair_results['first_component'], errors='coerce')
    pair_file = os.path.join(user_path, 
    "derived/auxiliary/pair_results.csv")
    pair_results.to_csv(pair_file)

    with open(dropped_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(cleaned_dropped3)

    G_loaded = confirmed_graph
    loaded_pair_results = pair_results
    loaded_cleaned_remaining3 = cleaned_remaining3
    loaded_dropped = cleaned_dropped3

else:
    with open(json_file, 'r') as f:
        graph_data = json.load(f)
        G_loaded = json_graph.node_link_graph(graph_data)
    
    with open(dropped_file, 'r', newline='') as file:
        reader = csv.reader(file)
        loaded_dropped = list(reader)

    loaded_cleaned_remaining3 = pd.read_csv(remaining_file)
    loaded_pair_results = pd.read_csv(pair_file)

    print('loaded')


new_graph, cleaned_remaining4 = cc.clean_results_pt4(G_loaded, 
loaded_cleaned_remaining3,
new_himss)

dropped_set = set(tuple(inner_list) for inner_list in loaded_dropped)
cleaned_remaining5, new_dropped = cc.clean_results_pt5(dropped_set,
cleaned_remaining4, new_himss, user_path)

contact_dict, contact_count_dict = cc.generate_pair_dicts(pair_results)

cc.update_confirmed_from_dropped(G_loaded,new_dropped,contact_count_dict)

new_himss['contact_uniqueid'] = new_himss['contact_uniqueid'].astype(int)
new_dropped, new_remaining = cc.clean_results_pt3(cleaned_remaining5, dropped_set,
user_path, new_himss,input_df)

cc.update_confirmed_from_dropped(G_loaded,new_dropped,contact_count_dict)

remaining_a = new_remaining[~((new_remaining['min_same_probability'] >=.5) &
                            (new_remaining['firstname_lev_distance'] <=1) & 
                            (new_remaining['lastname_lev_distance'] <=1))]


#cleaned_remaining13 = cleaned_remaining12[((
   #(cleaned_remaining12['min_probability_diff'] >=.25) | 
   #(cleaned_remaining12['min_probability'] >= .25)) & 
    #(((cleaned_remaining12['firstname_jw_distance'] >= .9) | 
   # (cleaned_remaining12['firstname_lev_distance']<= 1)) &
   # ((cleaned_remaining12['lastname_jw_distance'] >=.9) | 
   # (cleaned_remaining12['lastname_lev_distance']<=1)))) ] # i think this needs
    # to be a higher threshold because what if they have the same name


remaining_ids = pd.concat([remaining_a['contact_id1'],
                                remaining_a['contact_id2']])
cleaned_df = input_df[~input_df['contact_uniqueid'].isin(remaining_ids)]


# conditions
# cleaned_remaining3[cleaned_remaining3['id2_last_has_one_first'] & cleaned_remaining3['id1_last_has_one_first'] & (cleaned_remaining3['lastname_lev_distance']==0)]

new_grouped = cleaned_df.groupby(['last_meta', 'first_meta'])
pair_results_first = pd.concat([cc.find_pairwise_shared_attributes(sub_df, 
                                                             name_pairs_set,
                                                             meta_pairs_set) 
                                                             for _, 
                     sub_df in new_grouped])
pair_results_first = cc.update_results(pair_results_first) 

pair_file = os.path.join(user_path, 
    "derived/auxiliary/pair_results_meta.csv")
pair_results_first.to_csv(pair_file)

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


## ADD NEW RULES
cleaned_remaining5 = cleaned_remaining4[~(
    (cleaned_remaining4['firstname_lev_distance'] == 0) 
    & (cleaned_remaining4['lastname_lev_distance'] == 0) 
    & ((cleaned_remaining4['distinct_state_count'] == 1) |
    (cleaned_remaining4['shared_system_ids'])))]

cleaned_remaining6 = cleaned_remaining5[~(
    ((cleaned_remaining5['firstname_jw_distance'] >= 0.8) |
    (cleaned_remaining5['name_in_same_row_firstname']))
    & (cleaned_remaining5['lastname_jw_distance'] >= 0.8) 
    & ((cleaned_remaining5['shared_addresses_flag']) |
    (cleaned_remaining5['shared_entity_ids_flag']) |
     (cleaned_remaining5['shared_names_flag'])))]

cleaned_remaining7 = cleaned_remaining6[
    ~(((cleaned_remaining6['lastname_lev_distance'] > 2) 
    | (cleaned_remaining6['lastname_jw_distance'] <.8)) & 
    (cleaned_remaining6['both_F_and_M_present']))]

## tentative rules - might need more edge cases
#cleaned_remaining7[
    #(cleaned_remaining7['both_F_and_M_present']) &
    #(cleaned_remaining7['shared_states'].apply(len) == 0) &
    #~(cleaned_remaining7['shared_system_ids_flag'])]


test_graph, cleaned_remaining8 = cc.clean_results_pt4(test_graph, cleaned_remaining7,
new_himss)

# remaining are in cleaned_remaining8 or in remaining_a or in outliers
remaining_a_ids = set(pd.concat([remaining_a['contact_id1'],
                                remaining_a['contact_id2']]))
remaining_b_ids = set(pd.concat([cleaned_remaining7['contact_id1'],
                                cleaned_remaining7['contact_id2']]))

all_remaining_ids = outlier_ids.union(remaining_a_ids).union(remaining_b_ids)

remaining_ids = set(cleaned_df['contact_uniqueid'].unique()) - \
                confirmed_ids - set(test_graph.nodes())

common_nodes =  set(cleaned_df['contact_uniqueid'].unique()
                    ).union(confirmed_ids) - all_remaining_ids

G1_subgraph = confirmed_graph.subgraph(common_nodes).copy()
G_combined = nx.compose(G1_subgraph, test_graph)

final_confirmed_2 = cleaned_df[~cleaned_df['contact_uniqueid'].isin(all_remaining_ids)]
final_remaining_2 = cleaned_df[cleaned_df['contact_uniqueid'].isin(all_remaining_ids)]

final_confirmed_ids_2 = set(final_confirmed_2['contact_uniqueid'])

# 134025 and 1309084 should be the same at the end

graph_data = json_graph.node_link_data(G_combined)

# Write the JSON data to a file
new_confirmed_graph = os.path.join(user_path, "derived/auxiliary/new_graph.json")
with open(json_file, 'w') as f:
        json.dump(graph_data, f)

new_remaining_path = os.path.join(user_path, "derived/auxiliary/new_remaining.csv")
final_remaining_2.to_csv(remaining_file)

new_cleaned_path = os.path.join(user_path, "derived/auxiliary/new_cleaned.csv")
final_confirmed_2.to_csv(remaining_file)
