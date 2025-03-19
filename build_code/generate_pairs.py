import sys
import json
import pandas as pd
from joblib import Parallel, delayed
from itertools import chain
import os

# SET FILE PATHS
if len(sys.argv) == 10: # TAKE FILE PATHS FROM MAKEFILE

    confirmed_path = sys.argv[1]
    remaining_path = sys.argv[2]

    comp_pairs_path = sys.argv[3]
    meta_pairs_path = sys.argv[4]
    cleaned_meta_pairs_path = sys.argv[5]
    comp_single_block_path = sys.argv[6]
    meta_single_block_path = sys.argv[7]

    user_path = sys.argv[8] # data dir
    code_path = sys.argv[9]

else: # FILE PATHS FOR DEBUGGING
    print('WARNING - NOT USING MAKEFILE INPUT')
    user_path = "/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data"
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

# CREATE LAST META FIRST COMPONENT BLOCKS
grouped = input_df.groupby(['last_meta', 'first_component'])
block_results = Parallel(n_jobs=-1)(
    delayed(fuzz.parallel_blocks)(group) for _, group in grouped
)
block_component_pairs = pd.DataFrame(block_results)

# separate into ids that still need to be checked (remaining1)
cleaned1 = block_component_pairs[
    (block_component_pairs['contact_uniqueid'].apply(len) == 1)]
remaining1 = block_component_pairs[
    (block_component_pairs['contact_uniqueid'].apply(len) > 1)]

temp_cleaned_ids = set(chain.from_iterable(cleaned1['contact_uniqueid']))
remaining_ids = set(chain.from_iterable(remaining1['contact_uniqueid']))
comp_cleaned_ids = temp_cleaned_ids - remaining_ids 

# prepare for pairwise comparisons
filtered_df = input_df[input_df['contact_uniqueid'].isin(remaining_ids)]
name_pairs_set, meta_pairs_set = cc.gen_name_meta_pairs(user_path)

## LAST META, FIRST COMPONENT ROUND 1
new_grouped = filtered_df.groupby(['last_meta', 'first_component'])

# create comparisons within blocks
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

### CREATE LAST META FIRST META BLOCKS
new_grouped = input_df.groupby(['last_meta', 'first_meta'])
block_results2 = new_grouped.apply(lambda 
                                   sub_df: cc.find_common_blocks(sub_df, 
                                                                 first_meta=True)
                                                                 ).reset_index()

cleaned2 = block_results2[(block_results2['contact_uniqueid'].apply(len) == 1)]
remaining2 = block_results2[(block_results2['contact_uniqueid'].apply(len) > 1)]

cleaned_ids2 = set(chain.from_iterable(cleaned2['contact_uniqueid']))
remaining_ids2 = set(chain.from_iterable(remaining2['contact_uniqueid']))
meta_cleaned_ids = cleaned_ids2 - remaining_ids2

filtered_df = input_df[input_df['contact_uniqueid'].isin(remaining_ids2)]

new_grouped = filtered_df.groupby(['last_meta', 'first_meta'])
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
meta_pairs = pd.concat(results_list, ignore_index=True)


meta_pairs['contact_id_min'] = meta_pairs[['contact_id1', 
                                           'contact_id2']].min(axis=1)
meta_pairs['contact_id_max'] = meta_pairs[['contact_id1', 
                                           'contact_id2']].max(axis=1)

component_pairs['contact_id_min'] = component_pairs[['contact_id1', 
'contact_id2']].min(axis=1)
component_pairs['contact_id_max'] = component_pairs[['contact_id1', 
'contact_id2']].max(axis=1)

# Perform the merge on the sorted contact_id columns
merged_df = pd.merge(meta_pairs, component_pairs, 
                     on=['last_meta', 'contact_id_min', 'contact_id_max'], 
                     how='left', indicator=True)

all_meta_pairs = merged_df[merged_df['_merge'] == 
                           'left_only'].drop(columns=['_merge'])
all_meta_pairs = all_meta_pairs.loc[:, 
                                    ~all_meta_pairs.columns.str.endswith('_y')]
all_meta_pairs.columns = all_meta_pairs.columns.str.replace('_x$', '', 
                                                            regex=True)
all_meta_pairs = all_meta_pairs.drop(columns=['contact_id_min', 
                                              'contact_id_max'])

all_meta_pairs = fuzz.cast_types(all_meta_pairs)

all_meta_pairs['contact_id1'] = all_meta_pairs['contact_id1'].astype(str)
all_meta_pairs['contact_id2'] = all_meta_pairs['contact_id2'].astype(str)

# export dfs
component_pairs = fuzz.cast_types(component_pairs)
meta_pairs = fuzz.cast_types(meta_pairs)
all_meta_pairs = fuzz.cast_types(all_meta_pairs)

component_pairs.to_feather(comp_pairs_path)
meta_pairs.to_feather(meta_pairs_path)
all_meta_pairs.to_feather(cleaned_meta_pairs_path)

# export cleaned ids
with open(comp_single_block_path, "w") as f:
    json.dump(list(comp_cleaned_ids), f)

with open(meta_single_block_path, "w") as f:
    json.dump(list(meta_cleaned_ids), f)