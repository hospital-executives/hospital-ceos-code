import os
import sys 
import json
import pandas as pd
import networkx as nx
from itertools import chain

confirmed_path = sys.argv[1]
remaining_path = sys.argv[2]

final_cleaned_path = sys.argv[3]
final_remaining_path = sys.argv[4]
components_path = sys.argv[5]
user_path = sys.argv[6] # data dir
code_path = sys.argv[7]

print(f"Confirmed Path: {confirmed_path}")
print(f"Remaining Path: {remaining_path}")
print(f"Final Cleaned Path: {final_cleaned_path}")
print(f"Final Remaining Path: {final_remaining_path}")
print(f"Components Path: {components_path}")
print(f"Data Directory (User Path): {user_path}")
print(f"Code Directory: {code_path}")

raise ValueError

sys.path.append(os.path.join(os.path.dirname(__file__), 'helper-scripts'))
import blocking_helper
import cleaned_confirmed_helper as cc

# load dfs and set variable(s)
cleaned_confirmed = pd.read_csv(confirmed_path)
cleaned_remaining = pd.read_csv(remaining_path)
new_himss = pd.concat([cleaned_confirmed, cleaned_remaining], axis=0)
regenerate_hyphenated_pairs = None

###################### LAST META AND FIRST COMPONENT ######################
#### BLOCK LEVEL COMPARISONS ####
print('block level')
cleaned_confirmed['contact_uniqueid'] = cleaned_confirmed['contact_uniqueid'].apply(str)
grouped = cleaned_confirmed.groupby(['last_meta', 'first_component'])
block_results = grouped.apply(cc.find_common_blocks).reset_index() # takes 10 min

# separate into confirmed/remaining
cleaned1 = block_results[(block_results['contact_uniqueid'].apply(len) == 1)]
remaining1 = block_results[(block_results['contact_uniqueid'].apply(len) > 1)]

cleaned_ids = set(chain.from_iterable(cleaned1['contact_uniqueid']))
remaining_ids = set(chain.from_iterable(remaining1['contact_uniqueid'])) # 110754
unique_contact_ids = cleaned_ids - remaining_ids #83078

# final remaining from blockwise comparisons
filtered_df = cleaned_confirmed[cleaned_confirmed['contact_uniqueid'
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

## determine final confirmed/remaining
remaining_ids = pd.concat([cleaned_remaining3['contact_id1'],
                             cleaned_remaining3['contact_id2']])
remaining_ids_from_confirmed = set(remaining_ids.unique())
cleaned_ids_from_confirmed = set(confirmed_graph.nodes()) -  remaining_ids_from_confirmed

total_ids = set(cleaned_confirmed['contact_uniqueid'])
missing_ids = total_ids - cleaned_ids_from_confirmed - remaining_ids_from_confirmed

final_cleaned_from_confirmed = cleaned_ids_from_confirmed # 165,321
final_remaining_from_confirmed = remaining_ids_from_confirmed.union(missing_ids)

#### CLEAN UP + COMBINE REMAINDER DF ####
confirmed_ids = set(cleaned_confirmed['contact_uniqueid'].unique())
cleaned_remaining['contact_uniqueid'] = cleaned_remaining['contact_uniqueid'].apply(str)
remainder_ids = set(cleaned_remaining['contact_uniqueid'].unique())

outliers_path = os.path.join(user_path, "derived/auxiliary/outliers.csv")
outlier_df =  pd.read_csv(outliers_path)
outlier_df['contact_uniqueid'] = outlier_df['contact_uniqueid'].apply(str)
outlier_ids = set(outlier_df['contact_uniqueid'])

intersection = confirmed_ids & remainder_ids
intersection_list = list(intersection)

id_to_fullname = new_himss.groupby('contact_uniqueid')['full_name'].apply(
    lambda x: list(set(x))).to_dict()
name_to_ids = new_himss.groupby('full_name')['contact_uniqueid'].apply(
    lambda x: list(set(x)))
fullname_to_id = name_to_ids.to_dict()
names_with_multiple_ids = list(name_to_ids[name_to_ids.apply(len) > 1].index)

# identify matches if a contact ID is split between the R confirmed and remaining
cleaned_ids_across_dfs = cc.reconcile_ids_in_both_dfs(intersection_list, 
                                           cleaned_ids_from_confirmed, 
                                           name_pairs_set, cleaned_confirmed, 
                                           cleaned_remaining)
move_to_confirmed = cleaned_remaining[cleaned_remaining['contact_uniqueid'].isin(cleaned_ids)]
updated_true_remaining = cleaned_remaining[~
                                         cleaned_remaining['contact_uniqueid'].isin(cleaned_ids)]

#### CLEAN UP AND OUTPUT ####
print('clean')
confirmed_from_confirmed = cleaned_confirmed[cleaned_confirmed['contact_uniqueid'].isin(final_cleaned_from_confirmed)]
remaining_from_confirmed = cleaned_confirmed[cleaned_confirmed['contact_uniqueid'].isin(final_remaining_from_confirmed)]

final_remaining_ids = set(updated_true_remaining['contact_uniqueid']).union(
    set(remaining_from_confirmed['contact_uniqueid'])
).union(outlier_ids)

updated_himss = pd.concat([cleaned_confirmed, cleaned_remaining], axis=0)

final_confirmed = updated_himss[~updated_himss['contact_uniqueid'].isin(final_remaining_ids)]
final_remaining =  updated_himss[updated_himss['contact_uniqueid'].isin(final_remaining_ids)]


final_confirmed_ids = set(final_confirmed['contact_uniqueid'])
final_remaining_ids = set(final_remaining['contact_uniqueid'])


final_graph = confirmed_graph.subgraph(final_confirmed_ids).copy()

###################### LAST META AND FIRST METAPHONE ######################
print('last meta')
#### PAIRWISE LEVEL COMPARISONS ####
new_grouped = final_confirmed.groupby(['last_meta', 'first_meta'])
pair_results_first = pd.concat([cc.find_pairwise_shared_attributes(sub_df, 
                                                             name_pairs_set,
                                                             meta_pairs_set) 
                                                             for _, 
                     sub_df in new_grouped])
pair_results_first = cc.update_results(pair_results_first) 
merged_df = pd.merge(pair_results_first
, pair_results, on=['last_meta', 'contact_id1', 'contact_id2'], how='left', indicator=True)
test = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])
test = test.loc[:, ~test.columns.str.endswith('_y')]
test.columns = test.columns.str.replace('_x$', '', regex=True)

## need to determine which cases are clear
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

remaining_ids = set(final_confirmed['contact_uniqueid'].unique()) - \
                confirmed_ids - set(test_graph.nodes())

common_nodes =  set(final_confirmed['contact_uniqueid'].unique()
                    ).union(confirmed_ids) - remaining_ids

G1_subgraph = confirmed_graph.subgraph(common_nodes).copy()
G_combined = nx.compose(G1_subgraph, test_graph)
# 163k IDs
# with 162k "people"
final_confirmed_2 = final_confirmed[~final_confirmed['contact_uniqueid'].isin(remaining_ids)]
final_confirmed_ids_2 = set(final_confirmed_2['contact_uniqueid']) #163289

print('anomalies')
###################### LAST NAME ANOMALIES ######################
# hyphens
lastnames_with_space_or_hyphen = final_confirmed_2[
    final_confirmed_2['lastname'].fillna('').str.contains(r'[ -]', na=False)]

lastname_firstname_pairs = lastnames_with_space_or_hyphen[['lastname', 
                                                           'last_meta',
                                                           'firstname', 
                                                           'first_meta', 
                                                           'first_component']
                                                           ].drop_duplicates()
lasthyphens_dict = lastname_firstname_pairs.set_index('lastname')[['last_meta',
                                                                   'firstname', 
                                                                   'first_meta', 
                                                                   'first_component']
                                                                   ].apply(tuple, axis=1).to_dict()

if regenerate_hyphenated_pairs:
    pairs_set = cc.process_lastnames(lasthyphens_dict, final_confirmed_2, name_pairs_set)

    data_to_save = list(map(list, pairs_set))

    # Save to a JSON file
    with open('hyphenated_pairs.json', 'w') as f:
        json.dump(data_to_save, f)

to_add = set([('118339', '1368090'),
('1320101', '667700'),
('1331502', '673610'),
('1337815', '1334281'),
('1343799', '94641'), # check
('134750', '104848'),
('1349155', '2321573'),
('1355864', '103866'),
('1363949', '2326016'),
('1364149', '2340206'),
('1376092', '351372'),
('1389353', '2340206'),
('155719', '683743'),
('181439', '2323785'),
('182119', '1365880'),
('2305842', '512009'),
('2306823', '334560'),
('2312926', '794480'),
('2313079', '1369922'),
('2317657', '659666'),
('2318747', '351372'),
('2321084', '2307430'),
('2325162', '553758'),
('2332679', '596123'),
('2346588', '1351946'),
('2350981', '499020'),
('2364351', '1319082'),
('23663342', '1369897'),
('541934', '2339821'),
('630642', '1367609'), # check
('676640', '23657569'),
('793219', '1345582'),
('90989', '660356'),
('91876', '1309302')])

for id1,id2 in to_add:
    G_combined.add_edge(id1, id2)

###################### FINAL OUTPUT!!! ######################
print('almost there')
confirmed_ids = G_combined.nodes()

remaining_output = new_himss[~new_himss['contact_uniqueid'].isin(confirmed_ids)]
cleaned_output = new_himss[new_himss['contact_uniqueid'].isin(confirmed_ids)]
connections = {node: set(G_combined.neighbors(node)) for node in G_combined.nodes()}
connections_serializable = {key: list(value) for key, value in connections.items()}

# write dfs to csv
remaining_output.to_csv(final_remaining_path)
cleaned_output.to_csv(final_cleaned_path)
with open(components_path, 'w') as json_file:
    json.dump(connections_serializable, json_file, indent=4)
