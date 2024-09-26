import os
import sys
import pandas as pd

user_path = "/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data"

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from helper_scripts import cleaned_confirmed_helper as cc

# load files
confirmed_path =  os.path.join(user_path, "derived/auxiliary/py_confirmed.csv")
remainder_path =  os.path.join(user_path, "derived/auxiliary/py_remaining.csv")

py_confirmed = pd.read_csv(confirmed_path)
py_remainder = pd.read_csv(remainder_path)


name_pairs_set, meta_pairs_set = cc.gen_name_meta_pairs(user_path)

new_grouped = py_remainder.groupby(['last_meta', 'first_component'])
pair_results = pd.concat([cc.find_pairwise_shared_attributes(sub_df, 
                                                             name_pairs_set,
                                                             meta_pairs_set) 
                                                             for _, 
                     sub_df in new_grouped]) 

pair_results = cc.update_results(pair_results) 
contact_dict, contact_count_dict = cc.generate_pair_dicts(pair_results)

confirmed_graph, cleaned_dropped1, cleaned_remaining1 = cc.clean_results_pt1(
    pair_results)

cc.update_confirmed_from_dropped(confirmed_graph, cleaned_dropped1,
                                                 contact_count_dict)

cleaned_dropped2, cleaned_remaining2 = cc.clean_results_pt2(
    cleaned_remaining1, confirmed_graph, cleaned_dropped1, new_himss)

cc.update_confirmed_from_dropped(confirmed_graph, cleaned_dropped2,
                                                 contact_count_dict)

remaining_ids = pd.concat([cleaned_remaining2['contact_id1'],
                             cleaned_remaining2['contact_id2']])
remaining_ids_from_confirmed = set(remaining_ids.unique())

py_remainder['contact_uniqueid'] = py_remainder['contact_uniqueid'].astype(str)
test = py_remainder[~py_remainder['contact_uniqueid'].isin(remaining_ids_from_confirmed)]
print(len(test))