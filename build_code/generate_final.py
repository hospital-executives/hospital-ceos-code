import os
import sys
import json
import pandas as pd
import re
from datetime import datetime

user_path = "/Users/maggieshi/Dropbox/hospital_ceos/_data"
current_date = datetime.now().strftime("%m-%d")

#### SET VARIABLES ####
if len(sys.argv) == 7:
    confirmed_path = sys.argv[1]
    remainder_path = sys.argv[2]
    json_path = sys.argv[3]
    final_himss_path = sys.argv[4]
    final_confirmed_path = sys.argv[5]
    himss_path = sys.argv[6]

else: 
    print('WARNING: not using Makefile inputs')
    confirmed_path =  os.path.join(user_path, "derived/auxiliary/py_confirmed.csv")
    remainder_path =  os.path.join(user_path, "derived/auxiliary/py_remaining.csv")
    json_path =  os.path.join(user_path, "derived/auxiliary/py_graph_components.json")
    final_himss_path = os.path.join(user_path, "derived/final_himss.feather")
    final_confirmed_path = os.path.join(user_path, "derived/final_confirmed.dta")
    himss_path = os.path.join(user_path, 
    "derived/himss_entities_contacts_0517_v1.feather")

#### LOAD DATA ####
cleaned = pd.read_csv(confirmed_path)
cleaned['confirmed'] = True

remaining = pd.read_csv(remainder_path)
remaining['confirmed'] = False

new_himss = pd.concat([cleaned, remaining], axis=0)
new_himss = new_himss[new_himss['first_component'] != 'Unknown']

with open(json_path, 'r') as json_file:
    components_dict = json.load(json_file)  

#### REASSIGN IDS ####
converted_dict = {key: [int(item) for item in value] 
                  for key, value in components_dict.items() 
                  if len(value) > 0}

precomputed_max = {
    key: key if int(key) > max(value_list) else max(value_list)
    for key, value_list in converted_dict.items()
}

def map_to_max(id_value):
    return precomputed_max.get(id_value, id_value)

new_himss['new_contact_uniqueid'] = new_himss['contact_uniqueid'].astype(str).map(map_to_max)

#### ASSIGN FINAL BLOCKS ####
new_himss['first_component'] = new_himss['first_component'].astype(int)
max_first =  new_himss['first_component'].max()
new_himss['last_component'] = new_himss['last_component'] + max_first
new_himss['final_block'] = new_himss.apply(
    lambda row: row['first_component'] if row['gender'] == 'F' 
    else row['last_component'], axis=1)

name_to_meta_dict = new_himss.groupby('first_meta')['firstname'].apply(set).to_dict()

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

#### CLEAN AND EXPORT FINAL CLEANED HIMSS FILE ####
new_himss['medicarenumber'] = pd.to_numeric(new_himss['medicarenumber'], 
errors='coerce')
new_himss['ahanumber'] = new_himss['ahanumber'].astype(str)
new_himss['first_component'] = pd.to_numeric(new_himss['first_component'],
 errors='coerce')
new_himss['old_first_component'] = pd.to_numeric(new_himss['old_first_component'],
 errors='coerce')
new_himss['new_contact_uniqueid'] = new_himss['new_contact_uniqueid'].astype(str)


#### GENERATE FINAL CLEAN DF WITH FLAGS ####
new_cleaned = new_himss.copy()
himss = pd.read_feather(himss_path)

himss['id'] == himss['id'].astype(float)
himss['contact_uniqueid'] == himss['contact_uniqueid'].astype(float)

id_to_contact_uniqueid = himss.set_index('id')['contact_uniqueid'].to_dict()
float_dict = {float(k): float(v) for k, v in id_to_contact_uniqueid.items() if 
k is not None and v is not None}

new_cleaned['old_contact_uniqueid'] = new_cleaned['id'].map(float_dict)
new_cleaned['old_contact_uniqueid'] = new_cleaned['old_contact_uniqueid'].astype(float)

new_cleaned['mult_id_obs'] = new_cleaned.apply(
    lambda row: 1 if row['contact_uniqueid'] != 
    float_dict.get(row['id'], None) else 0, axis=1
)
new_cleaned['new_contact_uniqueid']= new_cleaned['new_contact_uniqueid'].astype(float)
new_cleaned['multname_multid_obs'] = new_cleaned.apply(
    lambda row: 1 if row['contact_uniqueid'] != 
    row['new_contact_uniqueid'] else 0, axis=1
)


# add mult name
def clean_fullname(name):
    # Remove punctuation using regex and convert to lowercase
    return re.sub(r'[^\w\s]', '', name).lower()

# Concatenate 'firstname' and 'lastname', add a space between them
himss['firstname'] = himss['firstname'].fillna('').astype(str)
himss['lastname'] = himss['lastname'].fillna('').astype(str)
himss['fullname'] = (himss['firstname'] + himss['lastname']).apply(clean_fullname)

name_counts = himss.groupby('contact_uniqueid')['fullname'].nunique()

mult_name_dict = {contact_id: 1 if count > 1 else 0 for contact_id, count in name_counts.items()}
int_key_dict = {float(k): v for k, v in mult_name_dict.items()}

new_cleaned['mult_name'] = new_cleaned['contact_uniqueid'].map(int_key_dict)

new_cleaned['mult_id_id'] = new_cleaned.groupby('contact_uniqueid')\
['mult_id_obs'].transform(lambda x: 1 if (x == 1).any() else 0)

new_cleaned['multname_multid_id'] = new_cleaned.groupby('new_contact_uniqueid')\
['multname_multid_obs'].transform(lambda x: 1 if (x == 1).any() else 0)

final_df = new_cleaned.copy()
final_df['contact_uniqueid'] = final_df['new_contact_uniqueid']
final_df = final_df.drop('old_contact_uniqueid', axis=1)
final_df = final_df.drop('new_contact_uniqueid', axis=1)

final_df['modified_firstname'] = final_df['firstname'] != final_df['old_firstname']
final_df['modified_lastname'] = final_df['lastname'] != final_df['old_lastname']

def convert_to_str_or_none(value):
    if isinstance(value, str):
        return value
    elif pd.isnull(value):
        return None
    else:
        return str(value)

# Apply the function to all object columns
object_columns = final_df.select_dtypes(include=['object']).columns.tolist()
for col in object_columns:
    final_df[col] = final_df[col].apply(convert_to_str_or_none)

final_df.to_feather(final_himss_path)
final_dta = final_df[final_df['confirmed']]
final_dta.to_stata(final_confirmed_path, write_index=False, version=118)


######
# CHATGPT SOLUTION BELOW
######

import shutil

# Save to a local temp file
temp_path = "/Users/maggieshi/Desktop/final_himss.feather"
final_df.to_feather(temp_path)

# Then move to Dropbox
shutil.move(temp_path, final_himss_path)

# doing the same thing witwh dta here, but 
final_dta = final_df[final_df['confirmed']]
final_dta.to_stata("/Users/maggieshi/Desktop/final_himss.dta", write_index=False, version=118)

shutil.move("/Users/maggieshi/Desktop/final_himss.dta", final_confirmed_path)