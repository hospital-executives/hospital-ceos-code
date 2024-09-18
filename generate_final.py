import os
import sys
import json
import pandas as pd
import numpy as np
from itertools import combinations

user_path = "/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data"

#### SET VARIABLES ####
if len(sys.argv) == 5:
    confirmed_path = sys.argv[1]
    remainder_path = sys.argv[2]
    json_path = sys.argv[3]
    final_path = sys.argv[4]
else: 
    confirmed_path =  os.path.join(user_path, "derived/py_confirmed.csv")
    remainder_path =  os.path.join(user_path, "derived/py_remaining.csv")
    json_path =  os.path.join(user_path, "derived/py_graph_components.json")
    final_path = os.path.join(user_path, "derived/final_himss.feather")

#### LOAD DATA ####
cleaned = pd.read_csv(confirmed_path)
cleaned['confirmed'] = True

remaining = pd.read_csv(remainder_path)
remaining['confirmed'] = False

new_himss = pd.concat([cleaned, remaining], axis=0)
new_himss = new_himss[new_himss['first_component'] != 'Unknown']

with open(json_path, 'r') as json_file:
    components_dict = json.load(json_file)  

# Reassign IDs
converted_dict = {key: [int(item) for item in value] 
                  for key, value in components_dict.items() 
                  if len(value) > 0}

precomputed_max = {
    key: max(value_list) 
    for key, value_list in converted_dict.items() 
    if any(val > int(key) for val in value_list)
}

def map_to_max(id_value):
    return precomputed_max.get(id_value, id_value)

new_himss['new_contact_uniqueid'] = new_himss['contact_uniqueid'].astype(str).map(map_to_max)

# assign final blocks
new_himss['first_component'] = new_himss['first_component'].astype(int)
max_first =  new_himss['first_component'].max()
new_himss['last_component'] = new_himss['last_component'] + max_first
new_himss['final_block'] = new_himss.apply(
    lambda row: row['first_component'] if row['gender'] == 'F' 
    else row['last_component'], axis=1)

name_to_meta_dict = new_himss.groupby('first_meta')['firstname'].apply(set).to_dict()

# alternatively can just reassign all of these to the male/na block
max_confirmed =  new_himss['final_block'].max()
specific_names = ["terry", "theresa", "terrance", "terrence", "teresa", "terri",
                  "tereza", "terrie", "therese", "teressa", "terese", "teri",
                  "theressa", 'tarry', 'terie', 'terre', 'terrye', 'taressa',
                'tereasa','teresa','terese', 'teressa','teresza','teriesa',
                'terisa','terissia', 'terresa','terrice','terris', 'tersa',
                'tersea','terence']
new_himss.loc[new_himss['firstname'].isin(specific_names), 
                    'final_block'] = max_confirmed + 1

specific_names = ["bobbie", "bobbi", "bobby", "bobbe", 'bobi'] 
new_himss.loc[new_himss['firstname'].isin(specific_names), 
                    'final_block'] = max_confirmed + 2

specific_names = ["KRS", "KRSTN", "KRST", "KRSTFR"]
new_himss.loc[new_himss['first_meta'].isin(specific_names), 
                    'final_block'] = max_confirmed + 3

new_himss['medicarenumber'] = pd.to_numeric(new_himss['medicarenumber'], 
errors='coerce')
new_himss['ahanumber'] = new_himss['ahanumber'].astype(str)
new_himss['first_component'] = pd.to_numeric(new_himss['first_component'],
 errors='coerce')
new_himss['old_first_component'] = pd.to_numeric(new_himss['old_first_component'],
 errors='coerce')
new_himss['new_contact_uniqueid'] = new_himss['new_contact_uniqueid'].astype(str)


# testing / debugging

#new_himss.to_feather(final_path)